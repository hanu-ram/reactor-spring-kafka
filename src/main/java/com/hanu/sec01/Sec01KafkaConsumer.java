package com.hanu.sec01;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Sec01KafkaConsumer {
    @SneakyThrows
    public static void main(String[] args) {
        Map<String, Object> consumerPropertiesMap = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "group-5",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-1",
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000
        );
        var options = ReceiverOptions.create(consumerPropertiesMap)
                .subscription(Collections.singleton("order-events"));
        KafkaReceiver<Object, Object> receiver = KafkaReceiver.create(options);
        CountDownLatch latch = new CountDownLatch(1);
        receiver.receive()
//                .parallel() // for parallel processing
//                .runOn(Schedulers.boundedElastic()) // for parallel processing
                .doOnNext(rec -> log.info("Key {} and value {}", rec.key(), rec.value()))
                .doOnNext(Sec01KafkaConsumer::process)
                .doOnNext(rec -> rec.receiverOffset().acknowledge())
                .doOnComplete(latch::countDown)
                .subscribe();

        latch.await();
    }
    @SneakyThrows
    public static void process(Object rec) {
        Thread.sleep(Duration.ofSeconds(3));
        log.info("Processing {}", rec);
    }
}
