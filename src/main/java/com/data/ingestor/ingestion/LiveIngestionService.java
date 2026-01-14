package com.data.ingestor.ingestion;

import com.data.ingestor.adapters.CandleEventAdapter;
import com.data.ingestor.binance.BackfillClient;
import com.data.ingestor.binance.ws.BinanceWsClient;
import com.data.ingestor.config.AppProperties;
import com.data.ingestor.kafka.CandleEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class LiveIngestionService {
    private static final Logger log = LoggerFactory.getLogger(LiveIngestionService.class);

    private final AppProperties props;
    private final BinanceWsClient wsClient;
    private final BackfillClient backfill;
    private final CandleEventAdapter adapter;
    private final CandleEventPublisher publisher;
    private final TopicResolver topicResolver;
    private final GapFillCoordinator gapFill;

    public LiveIngestionService(AppProperties props,
                                BinanceWsClient wsClient,
                                BackfillClient backfill,
                                CandleEventAdapter adapter,
                                CandleEventPublisher publisher,
                                TopicResolver topicResolver,
                                GapFillCoordinator gapFill) {
        this.props = props;
        this.wsClient = wsClient;
        this.backfill = backfill;
        this.adapter = adapter;
        this.publisher = publisher;
        this.topicResolver = topicResolver;
        this.gapFill = gapFill;
    }

    public Flux<Void> start() {
        return Flux.fromIterable(props.ingestion().symbols())
                .flatMap(this::startForSymbol);
    }

    private Flux<Void> startForSymbol(String symbol) {
        
    }
}
