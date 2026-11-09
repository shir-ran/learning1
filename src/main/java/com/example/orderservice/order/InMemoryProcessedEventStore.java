package com.example.orderservice.order;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

@Component
@Profile("in-memory")
public class InMemoryProcessedEventStore implements ProcessedEventStore {
    private final Set<UUID> processedEvents = new ConcurrentSkipListSet<>();


    @Override
    public boolean tryMarkAsProcessed(UUID eventId) {
        return processedEvents.add(eventId);
    }
}
