package com.example.orderservice.order;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderProcessingService {

    private final ProcessedEventStore processedEventStore;

    private final OrderProjectionRepository orderProjectionRepository;

    public OrderProcessingService(ProcessedEventStore processedEventStore, OrderProjectionRepository orderProjectionRepository) {
        this.processedEventStore = processedEventStore;
        this.orderProjectionRepository = orderProjectionRepository;
    }


    @Transactional // this means that of order projection insert fails the try processed is reverted as well.
    public void process(OrderCreated event){
        if (!processedEventStore.tryMarkAsProcessed(event.eventId())) {
            // log duplicate
            System.out.println("Duplicate event detected: " + event.eventId() + ". Skipping processing.");
            return;
        }

        if (event.totalAmount().longValue() > 250L) {
            // log invalid event
            System.out.println("Invalid event detected: " + event.eventId() + ". Skipping processing.");
            throw new InvalidAmountException("Order total amount exceeds limit: " + event.totalAmount() + " for event: " + event.eventId());
        }

        // perform/log business processing
        orderProjectionRepository.insert(event);
        System.out.println("event " + event.eventId() + "processed.");
    }
}
