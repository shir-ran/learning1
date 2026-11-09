# Lesson 3 — Producer acknowledgements and failures

Time box: 90 minutes.

## Outcome

Understand the difference between accepting a record into the local producer
buffer and receiving an acknowledgement from Kafka. Expose the asynchronous
send result, verify its metadata, and observe a real send failure.

By the end of this lesson, the flow will be:

```text
HTTP request
    -> OrderController
    -> OrderEventPublisher
    -> CompletableFuture<SendResult<...>>
    -> broker acknowledgement or exception
```

## Mental model

`KafkaTemplate.send(...)` does not synchronously wait for the complete network
operation. It returns a `CompletableFuture`.

```text
Application thread                 Kafka producer / broker
------------------                 -----------------------
send(record)
    -> local producer buffer
    <- CompletableFuture
                                     send batch
                                     leader writes record
                                     replicas acknowledge
future completes successfully  <-   acknowledgement

future completes exceptionally <-   timeout or permanent failure
```

The future completing successfully means that Kafka satisfied the configured
`acks` requirement. It does not mean that a consumer has processed the record.

## Exercise A — expose the send future

Change `OrderEventPublisher.publish` from `void` to:

```java
CompletableFuture<SendResult<String, OrderCreated>>
```

Return the future produced by `kafkaTemplate.send(...)`.

Required imports:

```java
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
```

The controller may temporarily ignore the returned future. This preserves the
current HTTP behavior while making the acknowledgement observable to callers
and tests.

## Exercise B — inspect successful acknowledgement metadata

Extend the Kafka integration test:

1. Publish a known `OrderCreated` event.
2. Wait for the future with a bounded timeout.
3. Inspect the returned `SendResult`.
4. Assert:
   - topic is `orders.created.v1`;
   - partition is between `0` and `2`;
   - offset is non-negative;
   - the produced record key equals `orderId`.

Important distinction:

```text
SendResult / RecordMetadata  -> broker acknowledged the write
Listener verification        -> a consumer processed the record
```

These are two different milestones.

## Exercise C — observe asynchronous failure

Create a focused test configuration that points a producer at an unavailable
broker and uses short delivery and blocking timeouts. Publish a record and
verify that the returned future completes exceptionally.

Questions:

1. Does `KafkaTemplate.send()` throw immediately? No, it took some time.
3. At what point does the future become exceptional?
4. What exception reaches the caller?
5. What would the current HTTP endpoint return while that failure is pending?

## Exercise D — decide the HTTP contract

Compare these designs:

### Fire and forget

The controller ignores the future and immediately returns `202 Accepted`.

- Low HTTP latency.
- The response does not prove Kafka accepted the record.
- A later failure needs logging, metrics, callbacks, or durable recovery.

### Wait for Kafka acknowledgement

The controller waits for the future before responding.

- The response reflects Kafka's acknowledgement.
- The request is coupled to Kafka latency and availability.
- Blocking without a timeout is dangerous.

### Transactional Outbox

The HTTP transaction stores the order and an outbox record in one database
transaction. A separate publisher sends the event to Kafka.

- The accepted order is durable even when Kafka is unavailable.
- More components and operational complexity.
- Common production solution for database-to-Kafka consistency.

We will not implement the outbox in this lesson; it becomes important once the
project adds persistent order storage.

## Producer configuration to understand

### `acks`

- `0`: do not wait for a broker acknowledgement.
- `1`: wait for the partition leader only.
- `all`: wait for all required in-sync replicas.

### `enable.idempotence`

Prevents duplicate log entries caused by producer retries within the producer
session. It does not make arbitrary application-level retries idempotent and it
does not make consumer side effects exactly once.

### `delivery.timeout.ms`

Upper bound on the time available for delivering a record, including retries.

### `max.block.ms`

Limits how long a call may block while waiting for metadata or producer buffer
space. This is different from waiting for the broker acknowledgement.

## Interview debrief

Be ready to explain:

1. What does the future returned by `KafkaTemplate.send()` represent? It represents the unfinished process of the kafka broker.
2. What is the difference between a producer acknowledgement and consumer
   processing? producer aknowledgement means the record was written to the kafka logs, consumer processing means it was read from kafka logs.
3. What durability does `acks=all` provide? It guarantees that all in-sync replicas received the record, and not just one of them or the lead er partition.
4. Why is idempotent producer behavior not equivalent to end-to-end exactly
   once processing? because it protects us from duplication only for the producer session and not the entire process or different sessions. It prevents duplicates created by producer retries within a producer session, but does not prevent application-level duplicate sends or duplicate consumer side effects.
5. Why can returning `202` immediately still lose an event? If the controller returns immediately 202 we can not know for sure that the record passed the producer stage and was written to the kafka logs.
6. When would a Transactional Outbox be preferable to waiting for Kafka? when we have a database to kafka element. When a business operation updates a database and must also publish an event. The state change and outbox row are committed in one database transaction, preventing “database committed but Kafka event lost.”

## Exercise E — verify effective Spring producer configuration

Inject the Spring `ProducerFactory` into the integration test and inspect
`getConfigurationProperties()`. Verify that the YAML was translated into:

```text
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
delivery.timeout.ms=120000
```

This catches misspelled or incorrectly nested YAML properties. It verifies the
configuration supplied by the application; Kafka may still add defaults while
creating the physical producer.

## Exercise F — reject incompatible idempotence configuration

Create a plain JUnit test with:

```text
enable.idempotence=true
acks=1
```

Creating the producer must fail with `ConfigException`. When idempotence is
explicitly enabled, Kafka fails fast instead of silently running with settings
that cannot provide the requested guarantee.

## Exercise G — separate application sends are not deduplicated

Publish the exact same event twice and wait for both acknowledgements. The
records use the same key and therefore reach the same partition, but Kafka
assigns different offsets.

```text
First publish()  -> producer sequence N     -> partition P, offset X
Second publish() -> producer sequence N + 1 -> partition P, offset X + 1
```

Producer idempotence deduplicates an internal retry of sequence `N`; it does not
deduplicate two application calls, because those are separate producer
operations with different sequence numbers.

## Session summary

- `KafkaTemplate.send()` returns before the broker operation is complete.
- Successful `SendResult` metadata proves that Kafka satisfied the configured
  acknowledgement requirement; it does not prove consumer processing.
- The HTTP endpoint now completes asynchronously with `202` after Kafka
  acknowledgement and `503` after publishing failure.
- `acks=all` and producer idempotence work together with retries and bounded
  in-flight requests.
- An appropriate replication factor and `min.insync.replicas` are still needed
  for broker-failure durability.
- Producer idempotence protects producer retries, not application-level duplicate
  calls or duplicate consumer side effects.
- The next reliability boundary is consumer idempotency.

