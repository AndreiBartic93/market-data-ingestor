package com.data.ingestor.ingestion;

import com.data.ingestor.config.AppProperties;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class TopicResolver {
    private final AppProperties props;

    public String topicFor(String symbol, String interval, boolean live) {
        if (props.kafka().separateLiveTopic() && live) {
            return props.kafka().candlesTopicPrefix() + ".live." + interval;
        }
        return props.kafka().candlesTopicPrefix() + "." + interval;
    }

    public String keyFor(String symbol, String interval, long openTime) {
        return symbol + "|" + interval + "|" + openTime;
    }
}
