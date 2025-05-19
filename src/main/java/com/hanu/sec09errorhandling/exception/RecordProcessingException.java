package com.hanu.sec09errorhandling.exception;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {
    private final ReceiverRecord<?, ?> receiverRecord;
    public RecordProcessingException(ReceiverRecord<?,?> receiverRecord, Throwable throwable) {
        super(throwable);
        this.receiverRecord = receiverRecord;
    }
    @SuppressWarnings("unchecked")
    public <K,V> ReceiverRecord<K, V> getReceiverRecord() {
        return (ReceiverRecord<K, V>) receiverRecord;
    }
}
