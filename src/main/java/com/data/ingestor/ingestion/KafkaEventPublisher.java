package com.data.ingestor.ingestion;

import com.data.ingestor.domain.websocket.CandleEventWs;
import com.data.ingestor.kafka.CandleEventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

@Component
@AllArgsConstructor
public class KafkaEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(CandleEventPublisher.class);

    private KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public Mono<Void> publish(String topic, String key, CandleEventWs event) {
        final String payload;
        try {
            payload = mapper.writeValueAsString(event);
        } catch(JsonProcessingException e) {
            return Mono.error(e);
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);

        return Mono.fromFuture(kafkaTemplate.send(record))
                .doOnSuccess(r -> log.debug("Published topic={} key={} type={}", topic, key event.eventType()))
                .then();
    }
}
