package com.example.orderservice.order;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public class OrderCommandRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderCommandRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OrderCreated event) {
        String sql = "INSERT INTO customer_order (order_id, customer_id, total_amount, created_at) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(sql, event.orderId(), event.customerId(), event.totalAmount(), Timestamp.from(event.occurredAt()));
    }
}
