package com.hanu.sec10kafkapoisonpillhandle;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Map;

/*
  goal: to consume from multiple topics
  producer ----> kafka broker <----------> consumer
 */

// poison pill
// publisher send the message in incorrect format like our consumer expecting string but publisher published the message in the integer in that kafka consumer will fail in order to handle this we have to use our custom deserializer
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "group-4",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.<String, Integer>create(consumerConfig)
                .withValueDeserializer(errorHandlingDeserializer())
                .subscription(Collections.singletonList("order-events"));
        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(header -> log.info("HeaderKey: {}, HeaderValue: {}", header.key(), new String(header.value()))))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }

    public static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer() {
        var errorHandlingDeserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
        errorHandlingDeserializer.setFailedDeserializationFunction(e -> {
            log.error("Error {}", new String(e.getData()));
            return -1000;
        });
        return errorHandlingDeserializer;
    }
}