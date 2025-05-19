package com.hanu.sec09;

import com.hanu.sec09.exception.RecordProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@RequiredArgsConstructor
@Slf4j
public class OrderEventProcessor {

    private final ReactiveDeadLetterTopicProducer<String, String> reactiveDeadLetterTopicProducer;

    public Mono<Void> process(ReceiverRecord<String, String> orderEvent) {
        return Mono.just(orderEvent)
                .doOnNext(r -> {
                    if (r.key().endsWith("5"))
                        throw new RuntimeException("processing exception");
                    log.info("key: {}, value: {}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(e -> new RecordProcessingException(orderEvent, e))
                .transform(reactiveDeadLetterTopicProducer.recordProcessingErrorHandler());
    }

}
