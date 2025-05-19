package com.hanu;

import com.hanu.sec01.Sec01KafkaConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@EmbeddedKafka(
//		ports = { 9092 },
		kraft = true,
		brokerProperties = { "auto.create.topics.enable=false" },
		topics = {"order-events"}
)
@Slf4j
class ReactorSpringKafkaApplicationTests {

	@Test
	void contextLoads() {
		var bootstrapServers = EmbeddedKafkaCondition.getBroker().getBrokersAsString();
		StepVerifier.create(Producer.run(bootstrapServers))
				.verifyComplete();

		StepVerifier.create(Consumer.run(bootstrapServers))
				.verifyComplete();
	}

	private static final class Producer {
		public static Mono<Void> run(String bootStrapServers) {

			var producerConfig = Map.<String, Object>of(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers,
					ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
					ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
			);

			var senderOptions = SenderOptions.<String, String>create(producerConfig);

			var fluxPublisher = Flux.range(1, 10)
					.map(i -> new ProducerRecord<>("order-events", "key-" + i, "order-" + i))
					.map(pr -> SenderRecord.create(pr, pr.key()));

			KafkaSender<String, String> kafkaSender = KafkaSender.create(senderOptions);
			return kafkaSender
					.send(fluxPublisher)
					.doOnNext(r -> log.info("Correlation Metadata:{}", r.correlationMetadata()))
					.doOnComplete(kafkaSender::close)
					.then();

		}
	}

	private static final class Consumer {
		public static Mono<Void> run(String bootStrapServers) {
			Map<String, Object> consumerPropertiesMap = Map.of(
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers,
					ConsumerConfig.GROUP_ID_CONFIG, "group-5",
					ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
					ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
					ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
					ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-1",
					ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000
			);
			var options = ReceiverOptions.create(consumerPropertiesMap)
					.subscription(Collections.singleton("order-events"));
			KafkaReceiver<Object, Object> receiver = KafkaReceiver.create(options);
			return receiver.receive()
					.take(10)
					.doOnNext(rec -> log.info("Key {} and value {}", rec.key(), rec.value()))
					.delayElements(Duration.ofMillis(10))
					.doOnNext(rec -> rec.receiverOffset().acknowledge()).then();
		}
	}

}
