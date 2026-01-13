package com.data.ingestor.ingestion;

import com.data.ingestor.binance.BackfillClient;
import com.data.ingestor.config.AppProperties;
import com.data.ingestor.kafka.TopicResolver;
import com.data.ingestor.kafka.CandleEventPublisher;
import com.data.ingestor.adapters.CandleEventAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class GapFillCoordinator {

    private static final Logger log = LoggerFactory.getLogger(GapFillCoordinator.class);

    private final Map<String, Long> lastClosedOpenTime = new ConcurrentHashMap<>();

    private final BackfillClient backfill;
    private final CandleEventAdapter adapter;
    private final CandleEventPublisher publisher;
    private final TopicResolver topicResolver;

    public GapFillCoordinator(BackfillClient backfill,
                              CandleEventAdapter adapter,
                              CandleEventPublisher publisher,
                              TopicResolver topicResolver) {
        this.backfill = backfill;
        this.adapter = adapter;
        this.publisher = publisher;
        this.topicResolver = topicResolver;
    }


}

