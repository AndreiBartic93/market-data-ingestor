package com.data.ingestor.ingestion;

import com.data.ingestor.adapters.CandleEventAdapter;
import com.data.ingestor.binance.BinanceRestClient;
import com.data.ingestor.config.AppProperties;
import com.data.ingestor.kafka.CandleEventPublisher;
import com.data.ingestor.kafka.TopicResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class IngestionJob {
    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);

    private final AppProperties props;
    private final BinanceRestClient binanceRestClient;
    private final CandleEventAdapter candleEventAdapter;
    private final CandleEventPublisher candleEventPublisher;
    private final TopicResolver topicResolver;

    //TODO: Use lombok
    public IngestionJob(AppProperties appProperties,
                        BinanceRestClient binanceRestClient,
                        CandleEventAdapter candleEventAdapter,
                        CandleEventPublisher candleEventPublisher,
                        TopicResolver topicResolver) {
        this.props = appProperties;
        this.binanceRestClient = binanceRestClient;
        this.candleEventAdapter = candleEventAdapter;
        this.candleEventPublisher = candleEventPublisher;
        this.topicResolver = topicResolver;
    }

    @Scheduled(fixedDelayString = "${app.ingestion.poll-seconds:60}000")
    public void pool() {

        log.info("props={}", props);
        log.info("ingestion={}", props.ingestion());

        var ingestion = props.ingestion();
        var limit = ingestion.limit();


        log.info("Scheduler tick: symbols={}, intervals={}, limit={}",
                ingestion.symbols(), ingestion.intervals(), ingestion.limit());

        Flux.fromIterable(ingestion.symbols())
                .flatMap(symbol ->
                        Flux.fromIterable(ingestion.intervals())
                                .flatMap(interval -> poolOne(symbol, interval, limit))
                        )
                .onErrorContinue((ex, obj) ->
                    log.error("Pooling pipeline error on {}", obj, ex)
                )
                .subscribe();
    }

    private Mono<Void> poolOne(String symbol, String interval, int limit) {
        String topic = topicResolver.topicForInterval(interval);

        return binanceRestClient.fetchKlines(symbol, interval, limit)
                .doOnSubscribe(s -> log.info("Pooling klines: symbol={}, interval={}, limit={} -> topic={}", symbol, interval, limit, topic))
                .flatMapMany(Flux::fromIterable)
                .map(kline -> candleEventAdapter.fromBinance(symbol, interval, kline))
                .concatMap(ev -> candleEventPublisher.publish(topic, ev))
                .then()
                .doOnError(e -> log.error("Pooling failed for symbol={}, interval={}, topic={}", symbol, interval, topic, e));


    }

//    @Scheduled(fixedDelayString = "${app.ingestion.pool-seconds:60}000")
//    public void pool() {
//        var ingestion = props.ingestion();
//        var topic = props.kafka().candlesTopic();
//
//        for (String symbol : ingestion.symbols()) {
//            binanceRestClient.fetchKlines(symbol, ingestion.intervals().get(0), ingestion.limit())
//                    .doOnSubscribe(s -> log.info("Pooling klines: simbol={}, intervals={}, limit={}",
//                            symbol, ingestion.intervals(), ingestion.limit()))
//                    .flatMapMany(list -> reactor.core.publisher.Flux.fromIterable(list))
//                    .map(k -> candleEventAdapter.fromBinance(symbol, ingestion.intervals().get(0), k))
//                    .doOnNext(ev -> candleEventPublisher.publish(topic, ev))
//                    .doOnError(e -> log.error("Polling failed for {}, symbol, e"))
//                    .subscribe();
//        }
//    }
}
