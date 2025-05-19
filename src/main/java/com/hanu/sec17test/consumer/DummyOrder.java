package com.hanu.sec17test.consumer;

public record DummyOrder(
        String orderId,
        String customerId
) {}