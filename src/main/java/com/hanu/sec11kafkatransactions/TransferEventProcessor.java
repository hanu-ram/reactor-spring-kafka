package com.hanu.sec11kafkatransactions;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Predicate;

@Slf4j
@AllArgsConstructor
public class TransferEventProcessor {
    private final KafkaSender<String, String> sender;

    public Flux<SenderResult<String>> process(Flux<TransferEvent> transferEventFlux) {
        return transferEventFlux
                .concatMap(this::validate)
                .concatMap(this::sendTransaction);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent transferEvent) {
        var senderRecords = toSenderRecords(transferEvent);
        var manager = sender.transactionManager();
        return manager.begin()
                .then(this.sender.send(senderRecords)
//                        .delayElements(Duration.ofSeconds(1)) alternative
                                .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(transferEvent.runnable())))
                                .concatWith(manager.commit())
                                .last()
                )
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> manager.abort());

    }

    private Mono<TransferEvent> validate(TransferEvent transferEvent) {
        return Mono.just(transferEvent)
                .filter(Predicate.not(t -> t.key().equals("5")))
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(transferEvent.runnable())
                                .doFirst(() -> log.error("Validation failed {}", transferEvent.key())
                                )
                );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent event) {
        var pr1 = new ProducerRecord<>("transaction-events", event.key(), "%s+%s".formatted(event.to(), event.amount()));
        var pr2 = new ProducerRecord<>("transaction-events", event.key(), "%s-%s".formatted(event.from(), event.amount()));
        var sr1 = SenderRecord.create(pr1, pr1.key());
        var sr2 = SenderRecord.create(pr2, pr2.key());
        return Flux.just(sr1, sr2);
    }
}
