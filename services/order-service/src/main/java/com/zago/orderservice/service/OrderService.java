package com.zago.orderservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zago.orderservice.dto.CreateOrderRequest;
import com.zago.orderservice.entity.Order;
import com.zago.orderservice.entity.OutboxEvent;
import com.zago.orderservice.repository.OrderRepository;
import com.zago.orderservice.repository.OutboxRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public UUID createOrder(CreateOrderRequest request) {

        UUID orderId = UUID.randomUUID();

        Order order = Order.builder()
                .id(orderId)
                .userId(request.getUserId())
                .amount(request.getAmount())
                .status("CREATED")
                .createdAt(Instant.now())
                .build();

        orderRepository.save(order);

        // EVENT PAYLOAD
        Map<String, Object> event = Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "OrderCreated",
                "eventVersion", "v1",
                "occurredAt", Instant.now().toEpochMilli(),
                "orderId", orderId.toString(),
                "userId", request.getUserId().toString(),
                "amount", request.getAmount()
        );

        JsonNode jsonNode = objectMapper.valueToTree(event);

        try {
            OutboxEvent outbox = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("orders")
                    .aggregateId(orderId)
                    .type("OrderCreated")
                    .payload(jsonNode)
                    .createdAt(Instant.now())
                    .build();

            outboxRepository.save(outbox);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return orderId;
    }
}
