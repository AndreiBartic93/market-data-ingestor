package com.data.ingestor.kafka;

import com.data.ingestor.config.AppProperties;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class TopicResolver {
    private final AppProperties appProperties;

    public String topicFor(String interval, boolean live) {
        String prefix = appProperties.kafka().candlesTopicPrefix();
        if (appProperties.kafka().separateLiveTopic() && live) {
            return prefix + ".live." + interval;
        }
        return prefix + "." + interval;
    }

    public String keyForClosed(String symbol, String interval, long openTime) {
        return symbol + "|" + interval + "|" + openTime;
    }

    public String keyForLive(String symbol, String interval) {
        return symbol + "|" + interval;
    }
}
