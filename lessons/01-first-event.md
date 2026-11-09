# Lesson 1 — Your first event flow

Time box: 90 minutes.

## Outcome

An HTTP request creates an `OrderCreated` event, Kafka stores it in one of three
partitions, and a Spring Kafka consumer receives it.

## Mental model (10 minutes)

- A **topic** is a named, durable event log.
- A **partition** is one ordered shard of that log.
- The **record key** decides which partition receives a record.
- An **offset** is a record's position inside one partition.
- A **consumer group** distributes partitions among its consumers.

Kafka guarantees ordering inside a partition, not across the whole topic. We use
`orderId` as the key so that future events for the same order keep their order.

## Run Kafka (10 minutes)

```powershell
docker compose up -d
docker compose ps
```

## Exercise A — implement the producer (20 minutes)

Open `OrderEventPublisher.java`. Replace the exception with a call to
`kafkaTemplate.send(...)` using:

- topic: `KafkaTopicConfig.ORDERS_CREATED_TOPIC`
- key: `event.orderId().toString()`
- value: `event`

Question to answer before coding: what undesirable behavior could occur if the
key were random for every event?
   If we used a random key for an order than we could accidentally send the event to several different partitions, 
   and since Kafka does not guarantee ordering across partitions, 
   we could end up with events for the same order being processed out of order.

## Exercise B — implement the consumer (15 minutes)

Add `@KafkaListener` to `OrderCreatedListener.onOrderCreated`. Subscribe to
`KafkaTopicConfig.ORDERS_CREATED_TOPIC` and use group id `order-service`.

## Run the application (10 minutes)

From IntelliJ, run `OrderServiceApplication`, or use IntelliJ's bundled Maven:

```powershell
& 'C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2023.1.3\plugins\maven\lib\maven3\bin\mvn.cmd' spring-boot:run
```

## Publish an order (10 minutes)

```powershell
$body = @{
  customerId = 'customer-101'
  totalAmount = 149.90
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri 'http://localhost:8080/orders' `
  -ContentType 'application/json' `
  -Body $body
```

The HTTP response should contain `orderId` and `eventId`. The application log
should then contain `Consumed OrderCreated`.

## Inspect Kafka (10 minutes)

```powershell
docker exec learning-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic orders.created.v1
```

Confirm that the topic has three partitions.

## Interview debrief (5 minutes)

Explain aloud:

1. Why did we use `orderId` as the Kafka key? to keep partition ordering affectively guaranteed for events related 
   to the same order, ensuring that all events for a specific order are sent to the same partition and processed
   in the correct sequence.
2. What does Kafka guarantee about ordering? ordering in any given partition, not the entire topic.
3. If we start five consumers in one group but have three partitions, how many
   consumers can actively receive records? all five consumers can receive records, but only three consumers will be 
   actively receiving records at any given time, as each partition can only be consumed by one consumer in the 
   group. The other two consumers will be idle until a rebalance occurs or a partition becomes available.

