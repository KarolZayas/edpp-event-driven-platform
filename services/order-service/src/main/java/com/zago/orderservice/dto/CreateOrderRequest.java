package com.zago.orderservice.dto;

import lombok.Data;

import java.util.UUID;

@Data
public class CreateOrderRequest {
    private UUID userId;
    private Double amount;
}
