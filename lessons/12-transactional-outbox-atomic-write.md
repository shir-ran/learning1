# Lesson 12 — Transactional outbox: atomic database write

Time box: 2 hours.

## Outcome

Replace the unsafe producer-side dual write with one PostgreSQL transaction:

```text
POST /orders
     |
     v
one database transaction
  ├── INSERT customer_order
  └── INSERT outbox_event containing OrderCreated JSON
```

At the end of this lesson, an accepted order and its event intent either both
exist or neither exists. Publishing the outbox to Kafka is intentionally left
for Session 13.

## Why the current flow is incomplete

The current endpoint sends directly to Kafka and waits for a broker
acknowledgement. It does not persist a producer-side order aggregate. A naive
attempt to add one creates a dual-write problem:

```text
INSERT order, then publish Kafka  -> Kafka can fail after the DB commits
publish Kafka, then INSERT order  -> the DB can fail after Kafka acknowledges
```

These are two independent systems. A normal PostgreSQL transaction cannot roll
back an already acknowledged Kafka record, and a Kafka producer transaction
cannot roll back a PostgreSQL commit.

The outbox pattern first records both the business change and the event intent
in the same database transaction. A separate relay later publishes committed
outbox rows.

## Exercise A — identify the consistency boundary

Before coding, answer:

1. If the order commits and Kafka is unavailable, what inconsistent state is
   visible? - the order exists, but no Kafka event exists. We do not want to delete 
   the valid order merely because Kafka is temporarily unavailable. 
   Instead, we persist an outbox row so publishing can be retried later.
2. If Kafka acknowledges and the order transaction rolls back, what
   inconsistent state is visible? - Kafka contains an event for an order that does 
   not exist. We generally cannot remove an individual acknowledged record from the 
   Kafka log. A consumer may also process it before any compensation is attempted.
3. Why does putting the order row and outbox row in one PostgreSQL transaction
   remove both failure windows? - if one insert fails the other is rolled back.
4. Does an outbox guarantee exactly-once Kafka publishing? Why not? - A unique event_id 
   prevents duplicate rows in the outbox table, but it does not prevent duplicate Kafka 
   publication. the standard transactional outbox usually provides at-least-once publication.

## Exercise B — add the producer-side tables

Create Flyway migration:

```text
src/main/resources/db/migration/V2__create_orders_and_outbox.sql
```

Add a `customer_order` table with:

- `order_id UUID PRIMARY KEY`
- `customer_id VARCHAR(255) NOT NULL`
- `total_amount NUMERIC(19, 2) NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`

Add an `outbox_event` table with:

- `event_id UUID PRIMARY KEY`
- `aggregate_type VARCHAR(100) NOT NULL`
- `aggregate_id UUID NOT NULL`
- `event_type VARCHAR(255) NOT NULL`
- `payload JSONB NOT NULL`
- `occurred_at TIMESTAMPTZ NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP`
- `published_at TIMESTAMPTZ NULL`

Add an index that helps a relay find unpublished rows in creation order:

```sql
CREATE INDEX idx_outbox_event_unpublished
    ON outbox_event(created_at)
    WHERE published_at IS NULL;
```

Do not add Kafka-specific partition or offset columns. The row represents an
event that must be published, not a record that has already been published.

## Exercise C — persist the order and event intent

Introduce these responsibilities:

```text
OrderCommandRepository
  insert the customer_order row

OutboxEventRepository
  serialize OrderCreated with ObjectMapper
  insert its metadata and JSON payload

OrderCommandService
  @Transactional create(CreateOrderRequest)
  generate orderId and eventId
  insert the order
  insert the outbox event
  return both IDs
```

Use the existing `OrderCreated` record as the outbox payload. Use these metadata
values consistently:

```text
aggregate_type = Order
event_type     = OrderCreated
aggregate_id   = orderId
```

The `@Transactional` boundary belongs on the service method because the service
owns the complete business operation. Individual repository transactions would
not make the two inserts atomic together.

Create a small return type, for example:

```java
public record CreatedOrder(UUID orderId, UUID eventId) {}
```

## Exercise D — change the HTTP contract

Change `OrderController` to call `OrderCommandService` instead of publishing
directly to Kafka. Return `202 Accepted` with `orderId` and `eventId` after the
database transaction commits.

The new acknowledgement means:

```text
The order and its event intent are durable in PostgreSQL.
```

It does not yet mean:

```text
Kafka acknowledged the event.
```

Update `OrderControllerTest` for the synchronous database-oriented contract.
The old `503 when Kafka publishing fails` test no longer describes the endpoint
and should be replaced rather than forced onto the new design.

## Exercise E — prove atomicity with PostgreSQL

Add a Testcontainers integration test that proves the success case:

1. Call the command service or `POST /orders`.
2. Assert exactly one matching row exists in `customer_order`.
3. Assert exactly one matching row exists in `outbox_event`.
4. Read `payload` and verify its `eventId`, `orderId`, `customerId`, and
   `totalAmount`.
5. Verify `published_at IS NULL` because no relay has published it yet.

Then prove rollback:

1. Make `OutboxEventRepository.insert(...)` throw a runtime exception using a
   test spy.
2. Call `OrderCommandService.create(...)` and assert that it fails.
3. Verify that no `customer_order` row remains.

The rollback assertion is the essential proof. A happy-path test alone does not
demonstrate the transactional guarantee.

## Interview debrief

1. What is the dual-write problem?
2. Where is the atomic consistency boundary in this implementation?
3. What does the HTTP `202` response guarantee now, and what does it not
   guarantee?
4. Why is the outbox payload stored with the business change?
5. Why is `published_at` initially null?
6. Why can the future relay still publish an event more than once?
7. Why must consumers remain idempotent even after introducing an outbox?

answers:
1. the dual write problem is when we have two sources of information - in our case the kafka and the DB and if one of them fails there will be an inconsistency between the sources. first state is record is in the DB but not in Kafka and vice versa.
2. The outbox transaction does not include Kafka. It includes an insert to the original DB table and an insert to the outbox event table.
3. the accepted response guarantees that the order and the outbox event are in the DB. it foes not guarantee that the record was published to kafka or handled by the listener.
4. the whole payload is saved so we don't need to reconstruct it later when we publish to kafka. we can just read the payload from the outbox table and send it to kafka.
5. published_at is initially null since we want to update it only after the relay mechanism got ack from the Kafka broker.
6. if the relay sent the publish but failed before update of the published at field, then the relay after the restart will send the same record again.
7. because of the twice published record we just mentioned the consumer must be idempotent.

## Completed outcome

- Added Flyway migration `V2__create_orders_and_outbox.sql` with the
  `customer_order` and `outbox_event` tables and a partial unpublished-event
  index.
- Implemented `OrderCommandService` with one Spring transaction around the
  order insert and outbox insert.
- Stored the complete `OrderCreated` payload as PostgreSQL `JSONB` together
  with aggregate and event metadata.
- Proved the success path by verifying the business row, outbox row, payload,
  and null `published_at` value against Testcontainers PostgreSQL.
- Proved rollback by forcing the outbox repository to fail after the order
  insert and verifying that no order row remained.
- Changed `POST /orders` so `202 Accepted` means the order and event intent are
  durable in PostgreSQL; it no longer waits for a Kafka acknowledgement.
- Verified that the REST request does not call the Kafka publisher directly.
- Preserved the previous direct-publisher implementation as commented learning
  history for comparison with the outbox-based contract.

## Next session

Build the relay that claims unpublished rows, sends them to Kafka, and marks
them published. Test the failure window between Kafka acknowledgement and the
`published_at` update.
