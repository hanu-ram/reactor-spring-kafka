package com.hanu.sec11kafkatransactions;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class TransferDemo {

    public static void main(String[] args) {
        var transferEventConsumer = new TransferEventConsumer(kafkaReceiver());
        var transferEventPublisher = new TransferEventProcessor(kafkaSender());
        transferEventConsumer.receive()
                .transform(transferEventPublisher::process)
                .doOnNext(t -> log.info("transfer success: {}", t))
                .doOnError(ex -> log.error("transfer failed: {}", ex.getMessage()))
        ;
    }
    private static KafkaReceiver<String, String> kafkaReceiver(){
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("transfer-requests"));
        return KafkaReceiver.create(options);
    }

    private static KafkaSender<String, String> kafkaSender(){
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "money-transfer"
        );
        var options = SenderOptions.<String, String>create(producerConfig);
        return KafkaSender.create(options);
    }
}
