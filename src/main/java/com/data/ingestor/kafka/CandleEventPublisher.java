package com.data.ingestor.kafka;

import com.data.ingestor.domain.CandleEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class CandleEventPublisher {

    private final KafkaTemplate<String, CandleEvent> kafkaTemplate;

    public CandleEventPublisher(KafkaTemplate<String, CandleEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<Void> publish(String topic, CandleEvent event) {
        return Mono.fromFuture(kafkaTemplate.send(topic, event).toCompletableFuture())
                .then();
    }
}
