package com.hanu.sec08;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/*
  goal: to consume from multiple topics
  producer ----> kafka broker <----------> consumer
 */
public class KafkaConsumerV2 {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerV2.class);

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
                .subscription(Pattern.compile("order.*"));
        KafkaReceiver.create(options)
                .receive()
                .flatMap(KafkaConsumerV2::batchProcess)
                .subscribe();
    }

    private static Mono<Void> batchProcess(ReceiverRecord<Object, Object> receiverRecord) {
        return Mono.just(receiverRecord)
//                .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> {
                    int index = ThreadLocalRandom.current().nextInt(2, 100); // 0-based
                    log.info("KafkaConsumerV2::batchProcess key: {}, index: {}, value: {}", r.key(), index, r.value().toString().charAt(index));
                })
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1)).onRetryExhaustedThrow((c, s) -> s.failure()))
                .doOnError(ex -> log.error(ex.getMessage()))
               /* .onErrorContinue((ec, d) -> {
//                    log.warn("Error: {}", ec.getMessage());
                    if (d instanceof ConsumerRecord<?, ?> consumerRecord) {
                        log.warn("Value is {}", consumerRecord.value());
                    }
                })*/
                .onErrorComplete()
                .doFinally(s -> receiverRecord.receiverOffset().acknowledge())
                .then();
    }
}