# Lesson 11 — Kafka consumer groups, rebalancing, and lag

Time box: 2 hours.

## Outcome

Use a local, cost-free topology to observe Kafka's runtime group behavior:

```text
orders.created.v1: 3 partitions

order-service-a: concurrency 3 ─┐
                                ├── group.id = order-service
order-service-b: concurrency 3 ─┘

order-producer: HTTP + Kafka producer, listeners disabled
```

We will inspect assignments, see why consumers become idle, deliberately create
lag while the group is empty, restart consumers to drain the backlog, and
observe rebalancing as group membership changes.

## Refresh from Session 10

- A CloudFormation template describes desired state. A stack is one deployed
  instance and owns the resources declared under `Resources`.
- `Parameters` supply inputs, `Resources` declare managed infrastructure, and
  `Outputs` expose named values to callers.
- A reference to another logical resource creates an implicit dependency.
- Identical desired state produces no change; an unrelated parameter update
  does not replace a resource that does not depend on it.
- A change set is the proposed diff. `Replacement: False` means the existing
  resource can be updated in place.
- The failed log update needed `logs:ListTagsForResource`; after updating IAM we
  used `ContinueUpdateRollback`, recovered the stack, and applied a fresh
  change set.
- Customer-managed IAM policies support larger documents, reuse, and versioned
  updates; inline policies belong to one identity and share a smaller aggregate
  size limit.

## Mental model

Within one consumer group, Kafka assigns each topic partition to at most one
consumer at a time. A consumer can own multiple partitions, but a partition is
not processed concurrently by two consumers in the same group.

```text
maximum useful consumer parallelism for one topic/group
    <= number of topic partitions
```

Spring's listener concurrency creates multiple Kafka consumer instances inside
one application process. Two application containers with concurrency `3`
therefore create six group members. With only three topic partitions, at least
three members must be idle.

A rebalance recalculates ownership when members join, leave, fail, or change
subscriptions. During assignment changes, affected partitions temporarily stop
being processed.

## Offsets and lag

For each partition:

```text
CURRENT-OFFSET   group's committed next position
LOG-END-OFFSET   broker's next position after the newest record
LAG              LOG-END-OFFSET - CURRENT-OFFSET
```

Lag measures how far a group is behind. It is not message age, processing time,
or a guarantee that the consumer is unhealthy. Interpret it together with its
trend, throughput, and expected traffic.

## Exercise A — inspect and start the topology

The session-specific file is `infra/session-11/compose.yaml`. It uses YAML
anchors to reuse Compose configuration; unlike CloudFormation, Docker Compose
supports YAML aliases.

Confirm the local image exists:

```powershell
docker image inspect order-service:session-08 --format "{{.Id}}"
```

Start the isolated Compose project:

```powershell
docker compose `
  -p session11 `
  -f infra/session-11/compose.yaml `
  up -d

docker compose `
  -p session11 `
  -f infra/session-11/compose.yaml `
  ps
```

Verify the three Spring processes:

```powershell
Invoke-RestMethod http://localhost:8080/actuator/health
Invoke-RestMethod http://localhost:8081/actuator/health
Invoke-RestMethod http://localhost:8082/actuator/health
```

The producer-only process starts Spring and KafkaTemplate but does not start any
`@KafkaListener` container because `spring.kafka.listener.auto-startup=false`.

## Exercise B — inspect partitions and group membership

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --describe `
  --topic orders.created.v1

docker compose -p session11 -f infra/session-11/compose.yaml exec kafka `
  /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 `
  --describe `
  --group order-service `
  --members `
  --verbose
```

Predict and then count:

```text
total group members
members with assignments
idle members
```

Client IDs identify whether a member belongs to service A or B.

## Exercise C — establish committed offsets

Publish six valid orders through the producer-only instance:

```powershell
1..6 | ForEach-Object {
    $body = @{
        customerId = "baseline-$($_)"
        totalAmount = 49.90
    } | ConvertTo-Json

    Invoke-RestMethod `
      -Method Post `
      -Uri http://localhost:8082/orders `
      -ContentType "application/json" `
      -Body $body |
      Out-Null
}
```

Describe group offsets:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml exec kafka `
  /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 `
  --describe `
  --group order-service
```

After consumption settles, lag should reach zero for all partitions.

## Exercise D — create and observe lag

Stop both consumer applications but leave Kafka, PostgreSQL, and the producer
running:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml `
  stop order-service-a order-service-b
```

Publish twelve more orders through port `8082`, then describe the consumer
group again. With no active members, log-end offsets advance while committed
offsets remain fixed, producing positive lag.

## Exercise E — drain lag and trigger rebalances

Start only service A:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml `
  up -d order-service-a
```

Observe its three members and poll the offset view until lag returns to zero.
Then start service B:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml `
  up -d order-service-b
```

Describe members again. Joining B triggers a rebalance: six members compete for
three partitions, so three are idle. Publish additional orders and compare:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml `
  logs --since 2m order-service-a order-service-b |
  Select-String "partitions assigned|partitions revoked|Consumed OrderCreated"
```

Because the sticky assignor may leave all three partitions on service A, stop A
while B remains running:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml `
  stop order-service-a
```

Describe the members again. The three remaining members from service B must now
take ownership of the three partitions. Publishing more orders demonstrates
that processing continues from the group's committed offsets on the replacement
instance rather than restarting at offset zero.

## Exercise F — cleanup

```powershell
docker compose `
  -p session11 `
  -f infra/session-11/compose.yaml `
  down --volumes
```

Confirm no Session 11 containers remain:

```powershell
docker compose -p session11 -f infra/session-11/compose.yaml ps -a
```

## Interview debrief

1. Why can only one consumer in a group own a partition at a time?
2. How do instance count and listener concurrency combine into group members?
3. Why does adding consumers beyond partition count not increase parallelism?
4. What events trigger a rebalance, and what work pauses during it?
5. How are committed offset, log-end offset, and lag related?
6. Why can lag be positive even when the consumer is healthy?
7. How does a message key affect partition choice and per-key ordering?
8. How would the behavior differ if service B used another group ID?

## Completed outcome

- Observed six group members competing for three partitions: three assigned and
  three idle.
- Published six baseline records and verified that committed offsets caught up
  with log-end offsets at zero lag.
- Stopped all consumers, published twelve records, and measured total lag of
  twelve while the group had no active members.
- Restarted service A and observed it resume from the group's committed offsets
  and drain the backlog to zero lag.
- Started service B and observed a rebalance that preserved A's valid sticky
  assignments, leaving B idle.
- Stopped service A and observed all three partitions fail over to service B.
- Published six more records and verified that B continued from the committed
  offsets, ending at twenty-four total records and zero lag.

## References

- [Kafka consumer-group operations](https://kafka.apache.org/44/operations/basic-kafka-operations/)
- [Spring Kafka listener concurrency](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html)
- [Spring Boot Kafka listener properties](https://docs.spring.io/spring-boot/3.5/appendix/application-properties/)

