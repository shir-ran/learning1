# Lesson 2 — Integration testing with a real Kafka broker

## Outcome

`mvn test` starts an isolated Kafka broker, starts Spring Boot, publishes an
`OrderCreated`, and verifies that the real listener consumed it.

## Exercise A — container lifecycle

Implement the `KafkaContainer` declaration in `KafkaContainerSmokeTest`.

Think about these annotations:

- `@Testcontainers` activates the JUnit 5 extension.
- `@Container` tells the extension which container it owns.
- A `static` container is started once for the test class, rather than once per
  test method.

Run only this test before adding Spring:

```powershell
& 'C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2023.1.3\plugins\maven\lib\maven3\bin\mvn.cmd' `
  -Dtest=KafkaContainerSmokeTest test
```

## Next

After the smoke test passes, connect Spring's dynamic
`spring.kafka.bootstrap-servers` property to the container and test the real
producer-to-consumer flow.
