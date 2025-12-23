package com.data.ingestor.kafka;

import com.data.ingestor.domain.CandleEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CandleEventPublisher {

    private final KafkaTemplate<String, CandleEvent> kafkaTemplate;

    public CandleEventPublisher(KafkaTemplate<String, CandleEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, CandleEvent event) {
        kafkaTemplate.send(topic, event.symbol(), event);
    }
}
