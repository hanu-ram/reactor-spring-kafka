package com.hanu.sec09;


import com.hanu.sec09.exception.RecordProcessingException;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

@AllArgsConstructor
public class ReactiveDeadLetterTopicProducer<K, V> {

    private final KafkaSender<K, V> kafkaSender;
    private final Retry retrySpec;

    public Mono<SenderResult<K>> publish(ReceiverRecord<K, V> receiverRecord) {
        var sr = toSenderRecord(receiverRecord);
        return this.kafkaSender.send(Mono.just(sr)).next();
    }

    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> receiverRecord) {
        var producerRecord = new ProducerRecord<>(
                receiverRecord.topic() + "-dlt",
                receiverRecord.key(),
                receiverRecord.value()
        );
        return SenderRecord.create(producerRecord, producerRecord.key());
    }

    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono -> mono
                .retryWhen(retrySpec)
                .onErrorMap(e -> e.getCause() instanceof RecordProcessingException, Throwable::getCause)
                .onErrorResume(RecordProcessingException.class, ex -> this.publish(ex.getReceiverRecord()).then(
                        Mono.fromRunnable(() -> ex.getReceiverRecord().receiverOffset().acknowledge())
                ))
                .then();
    }


}
