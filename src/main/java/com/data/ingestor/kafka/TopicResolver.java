package com.data.ingestor.kafka;

import com.data.ingestor.config.AppProperties;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class TopicResolver {
    private final AppProperties appProperties;

    public String topicForInterval(String interval) {
        String base = appProperties.kafka().candlesTopic();
        return base + "." + interval.toLowerCase();
    }
}
