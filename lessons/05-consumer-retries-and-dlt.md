# Lesson 5 — Consumer retries and dead-letter topics

Time box: 90 minutes.

## Outcome

Define what happens when processing an `OrderCreated` record repeatedly fails:

```text
initial delivery
    -> retry 1 after backoff
    -> retry 2 after backoff
    -> publish failed record to orders.created.v1.DLT
    -> allow the consumer group to continue
```

By the end of the lesson, an integration test will prove the number of delivery
attempts, consume the dead-letter record, and inspect its failure metadata.

## Refresh from Lesson 4

- `eventId` identifies a logical business event across offsets, partitions,
  republishes, and consumer restarts. The database unique constraint—not the ID
  by itself—enforces the idempotency decision.
- `INSERT ... ON CONFLICT DO NOTHING` performs the check and insert atomically,
  avoiding a check-then-act race between consumer threads or service instances.
- The processed marker and order projection share one transaction. If the
  business write fails, the marker rolls back and a later attempt may retry.
- Under a concurrent duplicate, one insert returns `1`; the conflicting
  `ON CONFLICT DO NOTHING` insert returns `0` and skips the side effect.
- Kafka can redeliver a record whose offset was not advanced. Spring Kafka's
  listener container and error handler determine the retry and recovery policy.

## Mental model

Not every failure should block a partition forever:

```text
transient failure
    database temporarily unavailable
    -> retry may succeed

permanent or poison-record failure
    value violates a business rule every time
    -> retries cannot fix it
    -> move to DLT after a bounded number of attempts
```

A dead-letter topic does not fix or delete the failed event. It isolates the
event so healthy records can continue and preserves information for alerting,
inspection, repair, and controlled replay.

## Exercise A — create the dead-letter topic

Add a constant and a `NewTopic` bean to `KafkaTopicConfig`:

```text
orders.created.v1.DLT
```

Give it three partitions, matching the source topic. We will deliberately send
each failed record to the same partition number as its source record.

Questions:

1. Why must the DLT have at least as many partitions when preserving the source
   partition number? Because we want to keep the partition number consistent for replay and ordering purposes. If the DLT had fewer partitions, some records would be forced into different partitions, potentially breaking the order of events for a given key.
2. Does preserving the partition also preserve the original offset? No, since not all records from the original topic will go to the DLT.

## Exercise B — configure bounded blocking retries

Create a Kafka consumer error-handling configuration with:

- `DeadLetterPublishingRecoverer`;
- an explicit destination resolver targeting `orders.created.v1.DLT` and the
  original partition;
- `DefaultErrorHandler`;
- `FixedBackOff(500L, 2L)`.

The second `FixedBackOff` argument is the number of **retries**, not the total
number of attempts:

```text
initial attempt + 2 retries = 3 listener invocations
```

These are blocking retries: the listener consumer thread waits during backoff,
and processing on the affected consumer thread does not move past the failed
record until it succeeds or is recovered to the DLT.

## Exercise C — force a deterministic processing failure

In `OrderKafkaIntegrationTest`, use the existing
`@MockitoSpyBean OrderProcessingService` to throw an
`IllegalStateException("simulated permanent failure")` only for one known
`eventId`.

Publish that event and verify:

```text
producer acknowledgement succeeds = yes
processing-service calls           = 3
processed_event rows               = 0
order_projection rows              = 0
```

The producer acknowledgement still succeeds because Kafka accepted the record.
Consumer processing is a separate milestone.

## Exercise D — consume and inspect the DLT record

Create a test consumer with a unique group ID and read one record from
`orders.created.v1.DLT`.

Verify:

- key equals the original `orderId`;
- value contains the original `eventId`;
- DLT partition equals the original source partition;
- headers identify the original topic, partition, and offset;
- exception headers describe the listener failure.

Spring Kafka's `DeadLetterPublishingRecoverer` enriches the DLT record with
original-record and exception headers. The DLT record itself receives a new
offset in the DLT; it does not retain the source topic's offset as its own.

## Exercise E — classify retryable and non-retryable failures

Discuss and configure one exception as non-retryable. A validation or permanent
business exception should usually go directly to recovery instead of consuming
time on attempts that cannot succeed. Infrastructure failures such as temporary
database unavailability are better retry candidates.

The policy should answer:

```text
Which exceptions retry?
How many times?
With what delay?
Where does the record go after exhaustion?
How will the team detect and replay it?
```

## Production considerations

- Blocking backoff pauses the consumer thread; long delays can reduce throughput
  and interact with consumer liveness settings.
- A DLT needs monitoring and ownership. An unread DLT is only hidden data loss.
- DLT replay must preserve or intentionally replace the event's idempotency key.
- Deserialization failures happen before a normal typed listener invocation and
  require `ErrorHandlingDeserializer` or an equivalent strategy.
- Publishing to the DLT can itself fail; recovery failure must not silently
  advance the source offset.
- A retry policy should include metrics for attempts, exhausted records, DLT
  publication failures, and DLT backlog.
  
While listener/consumer thread is waiting:
- It cannot process another record.
- It generally cannot process records from any other partition assigned to that same consumer thread.
- Records accumulate, increasing consumer lag.
- Throughput drops because the consumer spends time waiting rather than processing.

- The most relevant setting is `max.poll.interval.ms.` It limits the time allowed between calls to poll(). 
If processing and retry delays keep the consumer away from polling too long, Kafka considers it unresponsive 
and may rebalance its partitions to another consumer. The default is commonly five minutes.

`session.timeout.ms` is slightly different: it detects missing consumer heartbeats, commonly caused by 
a crashed process or broken connection. Kafka clients usually send heartbeats separately, so a slow 
listener is primarily a max.poll.interval.ms problem—not immediately a heartbeat problem.

For a record listener, Spring detects the deserialization-exception header and sends the failed record 
to the error handler without calling the typed listener. This enables recovery to a DLT.

## Interview debrief

Be ready to explain:

1. What is the difference between a retry and recovery?
Retry is that we define how many times and with what delay to retry the processing of a record when it fails. Recovery is the process of handling the record after all retry attempts have been exhausted, which may involve sending it to a dead-letter topic or logging the failure for further investigation.
2. Why is `FixedBackOff(500L, 2L)` three total attempts? the 2 is for 2 attempts of retry so there are totally 3 tries.
3. Why can a poison record block later records on a partition?
4. What information should a DLT record preserve?
5. Which failures should not be retried?
6. Why does a DLT require monitoring and a replay procedure?

A long backoff means the consumer thread or the listener is waiting a long time before trying another poll - therefore making the broker think it is not working and triggering the consumer group rebalance.
A business exception of invalid input is not a connection problem - another try won't solve the problem with the record - so there is no point to retry and instead we send the record to the DLT.

## References

- [Spring Kafka exception handling](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)
- [Spring Kafka DLT header API](https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/support/KafkaHeaders.html)

## Session summary

- A retry is another attempt to process the source record. Recovery is the
  action taken after attempts are exhausted or when an exception is classified
  as non-retryable.
- `FixedBackOff(500L, 2L)` means two retries after the initial delivery, for
  three total listener invocations.
- Blocking retry occupies the listener-container consumer thread during the
  backoff. It reduces throughput for partitions assigned to that thread and, if
  processing plus backoff keeps the consumer from polling beyond
  `max.poll.interval.ms`, can cause a consumer-group rebalance.
- `DeadLetterPublishingRecoverer` publishes an exhausted record to
  `orders.created.v1.DLT`, preserving the key and value and adding headers for
  the original topic, partition, offset, consumer group, and exception details.
- The DLT is a separate Kafka log. Its record has its own DLT offset; the source
  offset is metadata in a header.
- `InvalidAmountException` is non-retryable because repeating the same permanent
  business validation failure cannot make the record valid. It is recovered to
  the DLT after one processing attempt.
- A deserialization error happens while Kafka's bytes are being converted into
  an `OrderCreated`, before the typed listener can be invoked.
  `ErrorHandlingDeserializer` wraps the delegate deserializer, captures the
  exception and original bytes, and gives the listener container enough context
  to invoke the error handler without calling the typed listener.
- Tests prove both policies: a retryable exception produces three processing
  attempts before DLT recovery, while a non-retryable exception produces one.
  Database assertions prove failed processing leaves no processed marker or
  order projection.
- A DLT requires monitoring, alerts, clear ownership, investigation tooling,
  retention decisions, and a controlled replay procedure. Otherwise it merely
  turns visible processing failure into hidden data loss.
