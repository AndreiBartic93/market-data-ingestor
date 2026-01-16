package com.data.ingestor.ingestion;

import com.data.ingestor.adapters.CandleEventAdapter;
import com.data.ingestor.binance.BackfillClient;
import com.data.ingestor.binance.ws.BinanceWsClient;
import com.data.ingestor.config.AppProperties;
import com.data.ingestor.domain.websocket.BinanceWsKlineEvent;
import com.data.ingestor.kafka.CandleEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

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
        var intervals = props.ingestion().intervals();

        // 1) Bootstrap (opțional) ca să ai ultimul closed publicat + seed pentru gap-fill
        Mono<Void> bootstrap = bootstrapBackfill(symbol);

        // 2) WS stream
        Flux<BinanceWsKlineEvent> wsEvents = wsClient.klineCombinedStream(symbol, intervals)
                .map(msg -> msg.data())
                .doOnSubscribe(s -> log.info("WS started symbol={} intervals={}", symbol, intervals))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(props.ws().reconnectBackoffSeconds()))
                        .maxBackoff(Duration.ofSeconds(props.ws().maxReconnectBackoffSeconds()))
                        .doBeforeRetry(r -> log.warn("WS reconnect symbol={} attempt={} reason={}",
                                symbol, r.totalRetries() + 1, r.failure().toString())));

        Flux<Void> closeFlow = wsEvents
                .filter(ev -> ev.kline() != null && ev.kline().isClosed())
                .concatMap(this::handleClosed);

        Flux<Void> liveFlow = wsEvents
                .filter(ev -> ev.kline() != null && !ev.kline().isClosed())
                .groupBy(ev -> ev.symbol() + "|" + ev.kline().interval())
                .flatMap(this::publishLiveEverySecond); // price on each second

        return bootstrap.thenMany(Flux.merge(closeFlow, liveFlow));
    }

    private Mono<Void> handleClosed(BinanceWsKlineEvent ev) {
        String symbol = ev.symbol();
        String interval = ev.kline().interval();
        long openTime = ev.kline().openTime();

        var candle = adapter.fromWs(symbol, ev.kline());
        String topic = topicResolver.topicFor(symbol, interval, false);
        String key = topicResolver.keyFor(symbol, interval, openTime);

        return gapFill.gapFillIfNeeded(symbol, interval, openTime)
                .then(publisher.publish(topic, key, candle))
                .doOnSuccess(v -> gapFill.onClosedPublished(symbol, interval, openTime));
    }

    private Flux<Void> publishLiveEverySecond(GroupedFlux<String, BinanceWsKlineEvent> group) {
        // păstrăm "ultimul" update WS și emitem la 1s (chiar dacă nu vin trades)
        Flux<BinanceWsKlineEvent> latest = group.replay(1).refCount(1);

        return Flux.interval(Duration.ofSeconds(1))
                .withLatestFrom(latest, (tick, ev) -> ev)
                .concatMap(ev -> {
                    String symbol = ev.symbol();
                    String interval = ev.kline().interval();
                    long openTime = ev.kline().openTime();

                    var candle = adapter.fromWs(symbol, ev.kline());
                    String topic = topicResolver.topicFor(symbol, interval, true);
                    String key = topicResolver.keyFor(symbol, interval, openTime);

                    return publisher.publish(topic, key, candle);
                });
    }

    private Mono<Void> bootstrapBackfill(String symbol) {
        int bootstrapLimit = props.ingestion().bootstrapLimit();
        if (bootstrapLimit <= 0) {
            return Mono.empty();
        }

        return Flux.fromIterable(props.ingestion().intervals())
                .concatMap(interval -> {
                    long step = switch (interval) {
                        case "1h" -> 60 * 60_000L;
                        case "4h" -> 4 * 60 * 60_000L;
                        default -> 60 * 60_000L;
                    };
                    long end = System.currentTimeMillis();
                    long start = end - (long) bootstrapLimit * step;

                    return backfill.fetchKlines(symbol, interval, start, end, Math.min(bootstrapLimit, 1000))
                            .flatMapMany(Flux::fromIterable)
                            .sort((a, b) -> Long.compare(a.openTime(), b.openTime()))
                            .concatMap(k -> {
                                var ev = adapter.fromBackfill(symbol, interval, k);
                                String topic = topicResolver.topicFor(symbol, interval, false);
                                String key = topicResolver.keyFor(symbol, interval, ev.openTime());
                                return publisher.publish(topic, key, ev)
                                        .doOnSuccess(v -> gapFill.seedLastClosed(symbol, interval, ev.openTime()));
                            })
                            .then();
                })
                .then();
    }
}
