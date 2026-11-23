package com.example.orderservice.order;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

//    @MockitoBean
//    private OrderEventPublisher publisher;

    @MockitoBean
    private OrderCommandService orderCommandService;

    @Test
    @Disabled("Disabled until we implement the async Kafka publishing")
    void returnsServiceUnavailableWhenKafkaPublishingFails() throws Exception {
//        when(publisher.publish(any(OrderCreated.class)))
//                .thenReturn(CompletableFuture.failedFuture(
//                        new RuntimeException("Kafka unavailable")
//                ));

        var mvcResult = mockMvc.perform(
                        post("/orders")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                {
                                  "customerId": "customer-101",
                                  "totalAmount": 49.90
                                }
                                """)
                )
                .andExpect(request().asyncStarted())
                .andReturn(); // acknowledges that the request is handled asynchronously and returns without waiting

        // in the background, the controller will complete the request with a 503 Service Unavailable status due to the failed Kafka publishing
        mockMvc.perform(asyncDispatch(mvcResult)) //simulates the servlet container resuming the request after the future completes.
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void returnsAcceptedAfterOrderAndOutboxCommit() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        when(orderCommandService.create(any(CreateOrderRequest.class)))
                .thenReturn(new CreatedOrder(orderId, eventId));

        mockMvc.perform(
                        post("/orders")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                    {
                                      "customerId": "customer-101",
                                      "totalAmount": 49.90
                                    }
                                    """)
                )
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value(orderId.toString()))
                .andExpect(jsonPath("$.eventId").value(eventId.toString()));


        verify(orderCommandService).create(
                argThat(request ->
                        request.customerId().equals("customer-101")
                                && request.totalAmount()
                                .compareTo(new BigDecimal("49.90")) == 0
                )
        );
    }

    @Test
    void rejectsInvalidOrderBeforeCallingService() throws Exception {
        mockMvc.perform(
                        post("/orders")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                    {
                                      "customerId": "customer-101",
                                      "totalAmount": 0
                                    }
                                    """)
                )
                .andExpect(status().isBadRequest());

        verify(orderCommandService, never())
                .create(any(CreateOrderRequest.class));
    }
}