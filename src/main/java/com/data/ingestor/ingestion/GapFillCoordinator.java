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

    public Mono<Void> gapFillIfNeeded(String symbol, String interval, long newClosedOpenTimeMs) {
        String key = symbol + "|" + interval;
        Long last = lastClosedOpenTime.get(key);
        if (last == null) {
            return Mono.empty();
        }

        long step = intervalToMs(interval);
        long expectedNext = last + step;

        if (newClosedOpenTimeMs < expectedNext) {
            return Mono.empty();
        }

        long start = expectedNext;
        long endExclusive = newClosedOpenTimeMs;

        log.warn("GAP detected symbol={} interval={} last={} new={} -> backfill [{}..{})",
                symbol, interval, last, newClosedOpenTimeMs, start, endExclusive);

        long end = endExclusive - 1;
        return backfill.fetchKlines(symbol, interval, start, end, 1000)
                .flatMapMany(reactor.core.publisher.Flux::fromIterable)
                .concatMap(k -> {
                    var ev = adapter.fromBackfill(symbol, interval, k);
                    String topic = topicResolver.topicFor(symbol, interval, false);
                    String kafkaKey = topicResolver.keyFor(symbol, interval, ev.openTime());
                    return publisher.publish(topic, kafkaKey, ev);
                })
                .then();
    }

    public void onClosedPublished(String symbol, String interval, long openTimeMs) {
        lastClosedOpenTime.put(symbol + "|" + interval, openTimeMs);
    }

    public void seedLastClosed(String symbol, String interval, long openTimeMs) {
        lastClosedOpenTime.put(symbol + "|" + interval, openTimeMs);
    }

    private static long intervalToMs(String interval) {
        return switch (interval) {
            case "1m" -> 60_000L;
            case "5m" -> 5 * 60_000L;
            case "15m" -> 15 * 60_000L;
            case "1h" -> 60 * 60_000L;
            case "4h" -> 4 * 60 * 60_000L;
            case "1d" -> 24 * 60 * 60_000L;
            default -> throw new IllegalArgumentException("Unsupported interval: " + interval);
        };

}

