# Lesson 4 — Consumer idempotency

Time box: 90 minutes.

## Outcome

Understand why Kafka may deliver the same logical event more than once and make
the consumer's business side effect idempotent.

By the end of this lesson, the flow will be:

```text
Kafka record
    -> OrderCreatedListener
    -> check eventId
       -> new event: process the business operation once
       -> duplicate: skip the business operation
```

The goal is **effectively-once business processing**, even though Kafka itself
may deliver records at least once.

## Refresh from Lesson 3

- A successful `CompletableFuture<SendResult<...>>` proves that Kafka satisfied
  the configured producer acknowledgement requirement. It does not prove that
  any consumer processed the record.
- `max.block.ms` limits how long the application call may wait for producer
  metadata or local buffer space. It is not the broker acknowledgement timeout.
- `delivery.timeout.ms` is the overall deadline for delivering a record,
  including retries.
- Producer idempotence requires compatible settings such as `acks=all` so that
  sequence information survives leader failover consistently.
- Two explicit `publish()` calls are two legitimate sends with different
  producer sequence numbers, even if they happen in the same producer session.
  Producer idempotence only removes duplicate attempts caused by an internal
  retry of the same send.
- A Transactional Outbox is useful when one business operation must update a
  database and eventually publish a Kafka event without losing either part.

## Mental model

Producer idempotence and consumer idempotence solve different problems:

```text
Producer retry of send N
    -> producer idempotence prevents duplicate Kafka records

Two application sends, or consumer redelivery
    -> consumer idempotence prevents duplicate business side effects
```

A consumer can receive an event again when, for example, it performs the side
effect but crashes before its offset is committed. After restart or rebalance,
Kafka delivers the record again because the committed offset still points
before it.

```text
consume offset 10
    -> charge customer
    -> crash before committing offset 11

restart
    -> consume offset 10 again
    -> without idempotency: charge customer again
```

The offset identifies a position in one Kafka partition. The `eventId`
identifies the logical event across deliveries, so it is the better idempotency
key for the business operation.

## Exercise A — prove that the listener receives both records

The existing test `idempotenceDoesNotDeduplicateSeparatePublishCalls` already
publishes the same event twice and proves that Kafka stores it at two different
offsets.

Extend that test to also verify that `OrderCreatedListener.onOrderCreated(...)`
is called twice for that `eventId`.

Use the existing listener spy and Mockito's `times(2)` together with a bounded
`timeout(...)`. Match on `record.value().eventId()` rather than relying on the
partition or offset.

Expected result: the test passes. That is the problem we will solve—not a test
failure. Kafka correctly delivered both records because the application sent
two valid records.

After running the test, answer:

1. How many Kafka records were stored?
2. How many times did the listener run for the logical `eventId`?
3. If the listener charged a credit card, what would happen today?

## Exercise B — separate delivery from the business side effect

Create an `OrderProcessingService` Spring service with a method:

```java
void process(OrderCreated event)
```

For now, the method can log that it is processing the order. Inject this service
into `OrderCreatedListener` and call it from `onOrderCreated`.

Add a spy for `OrderProcessingService` to the integration test. Before adding
deduplication, verify that publishing the same event twice calls `process`
twice. This establishes a testable business side effect instead of treating a
log line as the outcome.

## Exercise C — add an atomic in-memory idempotency store

Create a `ProcessedEventStore` abstraction whose operation answers whether an
`eventId` was newly recorded. Implement the first version with a concurrent
in-memory set.

The check and insert must be one atomic operation. Avoid this unsafe pattern:

```text
if eventId is absent:
    add eventId
    process event
```

Two consumer threads could both observe "absent". A concurrent set's `add`
operation can atomically return `true` to only one caller.

Update the listener so that:

- a new `eventId` calls `OrderProcessingService.process`;
- an already-seen `eventId` is logged and skipped.

Then change the integration assertion:

```text
listener called twice
business service called once
```

This distinction is the heart of an idempotent consumer: duplicate **delivery**
is acceptable; duplicate **business effect** is not.

## Exercise D — identify the production limitation

The in-memory store teaches the algorithm, but it is not production-safe:

- it is empty after an application restart;
- separate application instances do not share it;
- marking the event and performing a database side effect are not one durable
  transaction;
- the set grows forever unless it has a retention policy.

A production implementation commonly stores `eventId` in a database table with
a unique constraint and writes the processed marker and business changes in the
same database transaction.

```text
database transaction
    -> INSERT processed_event(event_id)  // unique key
    -> apply business change
    -> COMMIT
```

If the insert violates the unique constraint, the event has already been
processed and the consumer can skip the side effect. We will build the durable
version after the in-memory behavior and tests are clear.

## Exercise E — move idempotency into the processing boundary

The Kafka listener should translate delivery into an application call; it
should not own the processed-event collection. Move the idempotency decision
behind `OrderProcessingService.process(...)`.

```text
OrderCreatedListener
    -> OrderProcessingService.process(event)
       -> ProcessedEventStore.tryMarkProcessed(eventId)
       -> new: apply business change
       -> duplicate: return without the business change
```

Introduce a `ProcessedEventStore` interface so the in-memory implementation can
later be replaced without changing the listener. Keep `tryMarkProcessed`
atomic.

## Exercise F — add PostgreSQL and schema migrations

Add Spring JDBC, PostgreSQL, Flyway, and PostgreSQL Testcontainers support. Run
PostgreSQL alongside Kafka in the integration test and register its JDBC URL,
username, and password through `@DynamicPropertySource`.

Create a Flyway migration containing:

```sql
CREATE TABLE processed_event (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_projection (
    order_id UUID PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    customer_id VARCHAR(255) NOT NULL,
    total_amount NUMERIC(19, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
```

`order_projection` gives us a real, observable business database effect. A log
message cannot participate in a database transaction.

## Exercise G — implement the durable idempotency boundary

Implement `ProcessedEventStore.tryMarkProcessed(eventId)` using one atomic SQL
statement:

```sql
INSERT INTO processed_event(event_id)
VALUES (?)
ON CONFLICT (event_id) DO NOTHING;
```

The JDBC update count answers whether this invocation inserted the marker:

- `1`: new event;
- `0`: duplicate event.

Make `OrderProcessingService.process(...)` transactional. Inside that one
transaction:

1. try to insert the processed-event marker;
2. return immediately when the event is a duplicate;
3. insert the order projection when it is new;
4. commit both writes together.

If inserting the projection fails, the marker must roll back too. Kafka can
then redeliver the event without the database falsely claiming it was already
processed.

## Exercise H — prove durable behavior

Publish the same event twice. Verify:

```text
Kafka records for eventId       = 2
listener invocations            = 2
processed_event rows            = 1
order_projection rows           = 1
```

This test proves business idempotency through durable state rather than through
a Mockito invocation count alone.

## Interview debrief

Be ready to explain:

1. Why can an at-least-once Kafka consumer receive the same record again? If a consumer failed after receiving the record and completing the side effects but before updating the offset for the broker - the same record can be consumed again.
2. Why is `eventId` usually a better idempotency key than a Kafka offset?
3. Why must "check whether processed" and "mark as processed" be atomic? because two threads can try to do it in the same time.
4. What is the crash window in an in-memory idempotency solution? A restart loses all markers. There is also no durable atomic transaction connecting the in-memory marker to the business database write: marking first can lose work after a crash, while applying the effect first can duplicate it after a crash.
5. How does a database unique constraint help multiple consumer instances? It arbitrates concurrent inserts across instances. One transaction inserts the event ID; another `ON CONFLICT DO NOTHING` insert returns an update count of `0` and skips the side effect.
6. Does an idempotent producer make the consumer idempotent? Why not? no, because we can have the crashing consumer before updating offset scenario even if every event happens only once.

## Session summary

- Kafka may redeliver a record, and an application may publish the same logical
  event as multiple Kafka records.
- `(topic, partition, offset)` identifies a physical record; `eventId`
  identifies the logical business event across deliveries and republishes.
- `INSERT ... ON CONFLICT DO NOTHING` combines the duplicate check and marker
  insertion into one atomic database operation. A separate read followed by an
  insert has a race condition.
- A PostgreSQL primary key coordinates duplicate detection across threads and
  application instances. Under contention, one transaction inserts the row;
  the other receives an update count of `0` after the conflict is resolved.
- The processed marker and order projection share one `@Transactional`
  boundary. If the business insert fails, the marker rolls back so a later
  delivery can retry.
- The listener may still run more than once. The achieved guarantee is one
  durable business effect per logical `eventId`, not one listener invocation or
  one effect per physical Kafka record.
- Tests prove that two Kafka offsets and two listener/service calls produce one
  `processed_event` row and one `order_projection` row.
- A rollback test proves that a failed projection insert leaves no false
  processed marker behind.
