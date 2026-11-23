package com.example.orderservice.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public class OutboxEventRepository {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxEventRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void insert(OrderCreated event)  {
        String payload = null;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox event", e);
        }
        String sql = "INSERT INTO outbox_event (event_id, aggregate_id, aggregate_type, event_type, payload, occurred_at) VALUES (?, ?, ?, ?, CAST(? AS JSONB), ?)";
        jdbcTemplate.update(sql,
                event.eventId(),
                event.orderId(),
                "Order",
                "OrderCreated",
                payload,
                Timestamp.from(event.occurredAt())
        );
    }
}
