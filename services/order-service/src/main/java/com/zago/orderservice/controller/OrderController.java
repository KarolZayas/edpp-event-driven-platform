package com.zago.orderservice.controller;

import com.zago.orderservice.dto.CreateOrderRequest;
import com.zago.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<?> create(@RequestBody CreateOrderRequest request) {
        UUID orderId = orderService.createOrder(request);
        return ResponseEntity.ok(Map.of("orderId", orderId));
    }
}
