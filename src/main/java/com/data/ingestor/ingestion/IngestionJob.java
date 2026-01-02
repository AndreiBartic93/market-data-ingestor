package com.data.ingestor.ingestion;

import com.data.ingestor.adapters.CandleEventAdapter;
import com.data.ingestor.binance.BinanceRestClient;
import com.data.ingestor.config.AppProperties;
import com.data.ingestor.kafka.CandleEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class IngestionJob {
    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);

    private final AppProperties props;
    private final BinanceRestClient binanceRestClient;
    private final CandleEventAdapter candleEventAdapter;
    private final CandleEventPublisher candleEventPublisher;

    public IngestionJob(AppProperties appProperties,
                        BinanceRestClient binanceRestClient,
                        CandleEventAdapter candleEventAdapter,
                        CandleEventPublisher candleEventPublisher) {
        this.props = appProperties;
        this.binanceRestClient = binanceRestClient;
        this.candleEventAdapter = candleEventAdapter;
        this.candleEventPublisher = candleEventPublisher;
    }

    @Scheduled(fixedDelayString = "${app.ingestion.pool-seconds:60}000")
    public void pool() {
        var ingestion = props.ingestion();
        var topic = props.kafka().candlesTopic();

        for (String symbol : ingestion.symbols()) {
            binanceRestClient.fetchKlines(symbol, ingestion.intervals(), ingestion.limit())
                    .doOnSubscribe(s -> log.info("Pooling klines: simbol={}, intervals={}, limit={}",
                            symbol, ingestion.intervals(), ingestion.limit()))
                    .flatMapMany(list -> reactor.core.publisher.Flux.fromIterable(list))
                    .map(k -> candleEventAdapter.fromBinance(symbol, ingestion.intervals(), k))
                    .doOnNext(ev -> candleEventPublisher.publish(topic, ev))
                    .doOnError(e -> log.error("Polling failed for {}, symbol, e"))
                    .subscribe();
        }
    }
}
