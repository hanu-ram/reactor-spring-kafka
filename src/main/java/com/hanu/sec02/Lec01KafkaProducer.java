package com.hanu.sec02;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;


@Slf4j
public class Lec01KafkaProducer {
    public static void main(String[] args) {

        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerConfig);

        var fluxPublisher = Flux.interval(Duration.ofMillis(100))
                .take(50)
                .map(i -> new ProducerRecord<>("order-events", "key-" + i, "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender<String, String> kafkaSender = KafkaSender.create(senderOptions);
        kafkaSender
                .send(fluxPublisher)
                .doOnNext(r -> log.info("Correlation Metadata:{}", r.correlationMetadata()))
                .doOnComplete(kafkaSender::close)
                .subscribe();

    }
}
