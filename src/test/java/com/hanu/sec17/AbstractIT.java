package com.hanu.sec17;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.function.UnaryOperator;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = { "order-events" }
)
public abstract class AbstractIT {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    protected <V> KafkaReceiver<String, V> createKafkaReceiver(String... topics) {
        return createKafkaReceiver(receiverOptions -> receiverOptions.withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new JsonDeserializer<V>().trustedPackages("*"))
                .subscription(List.of(topics))
        );
    }

    protected <K,V> KafkaReceiver<K, V> createKafkaReceiver(UnaryOperator<ReceiverOptions<K, V>> builder) {
        var consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        var receiverOptions = ReceiverOptions.<K,V>create(consumerProps);
        receiverOptions = builder.apply(receiverOptions);
        return KafkaReceiver.create(receiverOptions);
    }

}
