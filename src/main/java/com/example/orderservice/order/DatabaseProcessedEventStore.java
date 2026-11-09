package com.example.orderservice.order;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
@Profile("database")
public class DatabaseProcessedEventStore implements ProcessedEventStore {

    private final JdbcTemplate jdbcTemplate;

    public DatabaseProcessedEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean tryMarkAsProcessed(UUID eventId) {
        int insertedRows = jdbcTemplate.update("""
                INSERT INTO processed_event(event_id)
                VALUES (?)
                ON CONFLICT (event_id) DO NOTHING
                """, eventId);

        return insertedRows == 1;
    }
}
