package com.hanu.sec03;

import com.hanu.sec02.SubscriberImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Map;
import java.util.regex.Pattern;

/*
  goal: to consume from multiple topics
  producer ----> kafka broker <----------> consumer
 */
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "group-4",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*"));
        var subscriber = new SubscriberImpl<>();
        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(header -> log.info("HeaderKey: {}, HeaderValue: {}", header.key(), new String(header.value()))))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe(subscriber);

        subscriber.getSubscription().request(100);

    }

}