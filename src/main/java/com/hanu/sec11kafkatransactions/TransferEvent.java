package com.hanu.sec11kafkatransactions;

public record TransferEvent(
        String key,
        String from,
        String to,
        String amount,
        Runnable runnable
) {
}
