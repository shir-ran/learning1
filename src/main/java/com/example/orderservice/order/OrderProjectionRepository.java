package com.example.orderservice.order;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public class OrderProjectionRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderProjectionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OrderCreated event) {
        jdbcTemplate.update("""
                INSERT INTO order_projection(
                    order_id,
                    event_id,
                    customer_id,
                    total_amount,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                event.orderId(),
                event.eventId(),
                event.customerId(),
                event.totalAmount(),
                Timestamp.from(event.occurredAt())
        );
    }
}
