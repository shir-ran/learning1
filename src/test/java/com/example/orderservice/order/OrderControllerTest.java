package com.example.orderservice.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderEventPublisher publisher;

    @Test
    void returnsServiceUnavailableWhenKafkaPublishingFails() throws Exception {
        when(publisher.publish(any(OrderCreated.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new RuntimeException("Kafka unavailable")
                ));

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
    void returnsAcceptedAfterKafkaAcknowledges() throws Exception {
        CompletableFuture<SendResult<String, OrderCreated>> acknowledgement =
                new CompletableFuture<>();

        when(publisher.publish(any(OrderCreated.class)))
                .thenReturn(acknowledgement);

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
                .andReturn();

        acknowledgement.complete(mock(SendResult.class));

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").isString())
                .andExpect(jsonPath("$.eventId").isString());
    }
}