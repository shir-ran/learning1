package com.example.orderservice.order;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(properties = "spring.kafka.listener.concurrency=1")
@ActiveProfiles("database")
class OrderKafkaIntegrationTest {


    @Container
    static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:16-alpine");

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

    @MockitoSpyBean
    private OrderEventPublisher publisher;

    @MockitoSpyBean
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private MockMvc mockMvc;

    @MockitoSpyBean
    private OrderCreatedListener listener;

    @MockitoSpyBean
    private OrderProcessingService orderProcessingService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProducerFactory<String, OrderCreated> producerFactory;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OrderCommandService orderCommandService;

    @DynamicPropertySource
    static void configureKafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Test
    void startsKafkaBroker() {
        assertThat(kafka.isRunning()).isTrue();
        assertThat(kafka.getBootstrapServers())
                .matches("localhost:\\d+");
    }

    @Test
    void createsOrderProcessingTables() {
        Integer processedEvents = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_event",
                Integer.class
        );

        Integer orderProjections = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_projection",
                Integer.class
        );

        assertThat(jdbcTemplate.queryForObject(
                "SELECT to_regclass('public.processed_event')::text",
                String.class
        )).isEqualTo("processed_event");

        assertThat(jdbcTemplate.queryForObject(
                "SELECT to_regclass('public.order_projection')::text",
                String.class
        )).isEqualTo("order_projection");
    }

    @Test
    void publishesAndConsumesOrderCreated() throws ExecutionException, InterruptedException, TimeoutException {
        UUID orderId = UUID.randomUUID();
        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                "integration-test-customer",
                new BigDecimal("149.90"),
                Instant.now()
        );


        var futureResult = publisher.publish(event).get(10, java.util.concurrent.TimeUnit.SECONDS);
        verify(listener, timeout(10_000))
                .onOrderCreated(argThat(record ->
                                record.key().equals(orderId.toString())
                                && record.value().eventId().equals(event.eventId())
                ));
        //assertThat(futureResult).isCompleted();
        assertThat(futureResult.getRecordMetadata().topic()).isEqualTo("orders.created.v1");
        assertThat(futureResult.getRecordMetadata().partition()).isBetween(0,2);
        assertThat(futureResult.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0L);
        assertThat(futureResult.getProducerRecord().key()).isEqualTo(orderId.toString());


    }

    @Test
    void rejectsInvalidOrderWithoutPersistingIt() throws Exception {
        String invalidRequest = """
            {
              "customerId": "invalid-customer",
              "totalAmount": 0
            }
            """;

        mockMvc.perform(
                        org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/orders")
                                .contentType("application/json")
                                .content(invalidRequest)
                )
                .andExpect(status().isBadRequest());

//        verify(publisher, never())
//                .publish(any(OrderCreated.class));
        Integer orderCount = jdbcTemplate.queryForObject("""
        SELECT COUNT(*)
        FROM customer_order
        WHERE customer_id = 'invalid-customer'
        """,
                Integer.class
        );

        Integer outboxCount = jdbcTemplate.queryForObject("""
        SELECT COUNT(*)
        FROM outbox_event
        WHERE payload ->> 'customerId' = 'invalid-customer'
        """,
                Integer.class
        );

        assertThat(orderCount).isZero();
        assertThat(outboxCount).isZero();
    }

    @Test
    void usesDurableProducerConfiguration() {
        var properties = producerFactory.getConfigurationProperties();

        assertThat(properties.get(ProducerConfig.ACKS_CONFIG))
                .hasToString("all");

        assertThat(properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
                .hasToString("true");

        assertThat(properties.get(
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
        )).hasToString("5");

        assertThat(properties.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG))
                .hasToString("120000");
    }

    @Test
    void idempotenceDoesNotDeduplicateSeparatePublishCalls()
            throws Exception {

        UUID orderId = UUID.randomUUID();

        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                "duplicate-test-customer",
                new BigDecimal("49.90"),
                Instant.now()
        );

        var firstResult = publisher.publish(event)
                .get(10, TimeUnit.SECONDS);

        var secondResult = publisher.publish(event)
                .get(10, TimeUnit.SECONDS);

        assertThat(secondResult.getRecordMetadata().partition())
                .isEqualTo(firstResult.getRecordMetadata().partition());

        assertThat(secondResult.getRecordMetadata().offset())
                .isGreaterThan(firstResult.getRecordMetadata().offset());

        verify(listener, timeout(10_000).times(2))
                .onOrderCreated(argThat(record ->
                        record.value().eventId().equals(event.eventId())
                ));

        verify(orderProcessingService, timeout(10_000).times(2))
                .process(argThat(processedEvent ->
                        processedEvent.eventId().equals(event.eventId())
                ));

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Long processedEventRows = jdbcTemplate.queryForObject(
                            """
                            SELECT COUNT(*)
                            FROM processed_event
                            WHERE event_id = ?
                            """,
                            Long.class,
                            event.eventId()
                    );

                    Long projectionRows = jdbcTemplate.queryForObject(
                            """
                            SELECT COUNT(*)
                            FROM order_projection
                            WHERE event_id = ?
                            """,
                            Long.class,
                            event.eventId()
                    );

                    assertThat(processedEventRows).isEqualTo(1L);
                    assertThat(projectionRows).isEqualTo(1L);
                });


    }

    @Test
    void rollsBackProcessedMarkerWhenBusinessWriteFails() {
        UUID orderId = UUID.randomUUID();

        OrderCreated firstEvent = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                "customer-first",
                new BigDecimal("20.00"),
                Instant.now()
        );

        OrderCreated conflictingEvent = new OrderCreated(
                UUID.randomUUID(),
                orderId, // Same orderId causes order_projection PK conflict
                "customer-second",
                new BigDecimal("30.00"),
                Instant.now()
        );

        orderProcessingService.process(firstEvent);

        assertThatThrownBy(() ->
                orderProcessingService.process(conflictingEvent)
        ).isInstanceOf(DataIntegrityViolationException.class);

        Long conflictingMarkerCount = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM processed_event
                WHERE event_id = ?
                """,
                Long.class,
                conflictingEvent.eventId()
        );

        Long projectionCount = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM order_projection
                WHERE order_id = ?
                """,
                Long.class,
                orderId
        );

        assertThat(conflictingMarkerCount).isZero();
        assertThat(projectionCount).isEqualTo(1L);
    }

    @Test
    void retriesTwiceThenPublishesToDlt()
            throws Exception {

        UUID orderId = UUID.randomUUID();

        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                "failing-test-customer",
                new BigDecimal("49.90"),
                Instant.now()
        );



        doThrow(new IllegalStateException("simulated permanent failure"))
                .when(orderProcessingService).process(argThat(processedEvent ->
                        processedEvent.eventId().equals(event.eventId())
                ));

        var result = publisher.publish(event)
                .get(10, TimeUnit.SECONDS);

        verify(listener, timeout(10_000).times(3))
                .onOrderCreated(argThat(record ->
                        record.value().eventId().equals(event.eventId())
                ));
      verify(listener, timeout(10_000).times(1))
                .onOrderCorrupted(argThat(record ->
                        record.value().eventId().equals(event.eventId())
                        && record.value().orderId().equals(event.orderId())
                        && record.partition()== result.getRecordMetadata().partition()
                                && record.headers().lastHeader(
                                KafkaHeaders.DLT_ORIGINAL_TOPIC) != null
                                && record.headers().lastHeader(
                                KafkaHeaders.DLT_EXCEPTION_MESSAGE) != null
                ));

        verify(orderProcessingService, timeout(10_000).times(3))
                .process(argThat(processedEvent ->
                        processedEvent.eventId().equals(event.eventId())
                ));

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Long processedEventRows = jdbcTemplate.queryForObject(
                            """
                            SELECT COUNT(*)
                            FROM processed_event
                            WHERE event_id = ?
                            """,
                            Long.class,
                            event.eventId()
                    );

                    Long projectionRows = jdbcTemplate.queryForObject(
                            """
                            SELECT COUNT(*)
                            FROM order_projection
                            WHERE event_id = ?
                            """,
                            Long.class,
                            event.eventId()
                    );

                    assertThat(processedEventRows).isEqualTo(0L);
                    assertThat(projectionRows).isEqualTo(0L);
                });
    }

    @Test
    void routesInvalidAmountDirectlyToDltWithoutRetry()
            throws Exception {

        UUID orderId = UUID.randomUUID();

        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                "failing-test-customer",
                new BigDecimal("349.90"),
                Instant.now()
        );

        var result = publisher.publish(event)
                .get(10, TimeUnit.SECONDS);

//        doThrow(new InvalidAmountException("simulated permanent failure"))
//                .when(orderProcessingService).process(argThat(processedEvent ->
//                        processedEvent.eventId().equals(event.eventId())
//                ));


        verify(listener, timeout(10_000))
                .onOrderCorrupted(argThat(record ->
                        record.value().eventId().equals(event.eventId())
                                && record.value().orderId().equals(event.orderId())
                                && record.partition()== result.getRecordMetadata().partition()
                                && record.headers().lastHeader(
                                KafkaHeaders.DLT_ORIGINAL_TOPIC) != null
                                && record.headers().lastHeader(
                                KafkaHeaders.DLT_EXCEPTION_MESSAGE) != null
                ));

        verify(listener, times(1))
                .onOrderCreated(argThat(record ->
                        record.value().eventId().equals(event.eventId())
                ));

        verify(orderProcessingService, times(1))
                .process(argThat(processedEvent ->
                        processedEvent.eventId().equals(event.eventId())
                ));

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Long processedEventRows = jdbcTemplate.queryForObject(
                            """
                            SELECT COUNT(*)
                            FROM processed_event
                            WHERE event_id = ?
                            """,
                            Long.class,
                            event.eventId()
                    );

                    Long projectionRows = jdbcTemplate.queryForObject(
                            """
                            SELECT COUNT(*)
                            FROM order_projection
                            WHERE event_id = ?
                            """,
                            Long.class,
                            event.eventId()
                    );

                    assertThat(processedEventRows).isEqualTo(0L);
                    assertThat(projectionRows).isEqualTo(0L);
                });
    }

    @Test
    void storesOrderAndOutboxEventAtomically() {
        var result = orderCommandService.create(
                new CreateOrderRequest(
                        "outbox-customer",
                        new BigDecimal("79.90")
                )
        );

        Integer orderCount = jdbcTemplate.queryForObject("""
            SELECT COUNT(*)
            FROM customer_order
            WHERE order_id = ?
            """,
                Integer.class,
                result.orderId()
        );

        Integer outboxCount = jdbcTemplate.queryForObject("""
            SELECT COUNT(*)
            FROM outbox_event
            WHERE event_id = ?
              AND aggregate_id = ?
              AND published_at IS NULL
            """,
                Integer.class,
                result.eventId(),
                result.orderId()
        );

        assertThat(orderCount).isEqualTo(1);
        assertThat(outboxCount).isEqualTo(1);

        Map<String, Object> outboxRow = jdbcTemplate.queryForMap("""
        SELECT aggregate_type,
               event_type,
               payload ->> 'eventId' AS payload_event_id,
               payload ->> 'orderId' AS payload_order_id,
               payload ->> 'customerId' AS payload_customer_id,
               payload ->> 'totalAmount' AS payload_total_amount
        FROM outbox_event
        WHERE event_id = ?
        """,
                result.eventId()
        );

        assertThat(outboxRow.get("aggregate_type")).isEqualTo("Order");
        assertThat(outboxRow.get("event_type")).isEqualTo("OrderCreated");
        assertThat(outboxRow.get("payload_event_id"))
                .isEqualTo(result.eventId().toString());
        assertThat(outboxRow.get("payload_order_id"))
                .isEqualTo(result.orderId().toString());
        assertThat(outboxRow.get("payload_customer_id"))
                .isEqualTo("outbox-customer");

        assertThat(new BigDecimal(
                (String) outboxRow.get("payload_total_amount")
        )).isEqualByComparingTo("79.90");
    }

    @Test
    void rollsBackOrderWhenOutboxInsertFails() {
        String customerId = "rollback-" + UUID.randomUUID();

        doThrow(new IllegalStateException("Simulated outbox failure"))
                .when(outboxEventRepository)
                .insert(argThat(event ->
                        event.customerId().equals(customerId)
                ));

        assertThatThrownBy(() ->
                orderCommandService.create(
                        new CreateOrderRequest(
                                customerId,
                                new BigDecimal("59.90")
                        )
                )
        )
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Simulated outbox failure");

        Integer remainingOrders = jdbcTemplate.queryForObject("""
            SELECT COUNT(*)
            FROM customer_order
            WHERE customer_id = ?
            """,
                Integer.class,
                customerId
        );

        assertThat(remainingOrders).isZero();
    }

    @Test
    void persistsOrderAndOutboxViaRest() throws Exception {
        var result = mockMvc.perform(
                        post("/orders")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                    {
                                      "customerId": "http-outbox-customer",
                                      "totalAmount": 149.90
                                    }
                                    """)
                )
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").isString())
                .andExpect(jsonPath("$.eventId").isString())
                .andReturn();

        var response = objectMapper.readTree(
                result.getResponse().getContentAsString()
        );

        UUID orderId = UUID.fromString(
                response.get("orderId").asText()
        );

        UUID eventId = UUID.fromString(
                response.get("eventId").asText()
        );

        Integer orderCount = jdbcTemplate.queryForObject("""
            SELECT COUNT(*)
            FROM customer_order
            WHERE order_id = ?
              AND customer_id = 'http-outbox-customer'
              AND total_amount = 149.90
            """,
                Integer.class,
                orderId
        );

        Integer outboxCount = jdbcTemplate.queryForObject("""
            SELECT COUNT(*)
            FROM outbox_event
            WHERE event_id = ?
              AND aggregate_id = ?
              AND payload ->> 'orderId' = ?
              AND payload ->> 'customerId' = 'http-outbox-customer'
              AND published_at IS NULL
            """,
                Integer.class,
                eventId,
                orderId,
                orderId.toString()
        );

        assertThat(orderCount).isEqualTo(1);
        assertThat(outboxCount).isEqualTo(1);

        verify(publisher, never())
                .publish(any(OrderCreated.class));
    }
}
