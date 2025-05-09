package com.hanu.sec07;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Map;
import java.util.regex.Pattern;
@Slf4j
public class FlatMapWithGroupByParallelProcess {
    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "group-5",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2")
                .subscription(Pattern.compile("order.*"));
        KafkaReceiver.create(options)
                .receive()
                .groupBy(r -> r.key().hashCode() % 5)
                .flatMap(FlatMapWithGroupByParallelProcess::batchProcess)
                .subscribe();

    }
    private static Mono<Void> batchProcess(GroupedFlux<Integer, ReceiverRecord<Object, Object>> flux) {
        return flux.doFirst(() -> log.info("------------------------------"))
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(header -> log.info("HeaderKey: {}, HeaderValue: {}", header.key(), new String(header.value()))))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }
}
