package com.hanu.sec17;

import com.hanu.sec17test.producer.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import java.time.Duration;

@TestPropertySource(properties = "app=producer")
@Slf4j
class OrderEventProducerTest extends AbstractIT {

    @Test
    void producerTest1() {
        KafkaReceiver<String, OrderEvent> kafkaReceiver = createKafkaReceiver("order-events");
        Flux<ReceiverRecord<String, OrderEvent>> receiverRecordFlux = kafkaReceiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()));

        StepVerifier.create(receiverRecordFlux)
//                .assertNext(r -> Assertions.assertNotNull(r.value().orderDate()))
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderId()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(12));
    }

}
