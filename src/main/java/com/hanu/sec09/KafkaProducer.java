package com.hanu.sec09;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;


@Slf4j
public class KafkaProducer {
    public static void main(String[] args) {

        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerConfig);
//          .maxInFlight(10_000); this can improve the throughput of the application by emitting the events from the FLux or Mono producer faster by extending the queue size.
//                it will be using in the limitRate(10_000) function in reactive java


        var fluxPublisher = Flux.range(1, 100)
                .map(KafkaProducer::createSenderRecord);

        KafkaSender<String, String> kafkaSender = KafkaSender.create(senderOptions);
        kafkaSender
                .send(fluxPublisher)
                .doOnNext(r -> log.info("Correlation Metadata:{}", r.correlationMetadata()))
                .doOnComplete(kafkaSender::close)
                .subscribe();
    }

    private static SenderRecord<String, String, String> createSenderRecord(int i) {
        var headers = new RecordHeaders();
        headers.add("clientid-header", "clientid".getBytes());
        var producerRecord = new ProducerRecord<>("order-events", null, "key-" + i, "order-" + i, headers);
        return SenderRecord.create(producerRecord, producerRecord.key());
    }
}
