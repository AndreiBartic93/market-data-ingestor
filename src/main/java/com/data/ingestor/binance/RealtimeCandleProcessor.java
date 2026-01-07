package com.data.ingestor.binance;

import com.data.ingestor.adapters.CandleEventAdapter;
import com.data.ingestor.domain.websocket.BinanceWsKlineEvent;
import com.data.ingestor.domain.websocket.CandleInterval;
import com.data.ingestor.domain.websocket.StreamKey;
import com.data.ingestor.kafka.CandleEventPublisher;
import com.data.ingestor.kafka.TopicResolver;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
public class RealtimeCandleProcessor {

    private static final Logger log = LoggerFactory.getLogger(RealtimeCandleProcessor.class);
    private final Map<StreamKey, Long> lastPublishedOpenTime = new ConcurrentHashMap<>();

    private final BackfillClient backfillClient;
    private final CandleEventAdapter adapter;
    private final CandleEventPublisher publisher;
    private final TopicResolver topicResolver;

    public Mono<Void> processClosedKline(BinanceWsKlineEvent ws) {
        String symbol = ws.symbol();
        CandleInterval interval = CandleInterval.from(ws.kline().interval());
        StreamKey key = new StreamKey(symbol, interval);

        long openTime = ws.kline().openTime();

        long last = lastPublishedOpenTime.getOrDefault(key, -1L);
        if (last >= 0 && openTime <= last) {
            return Mono.empty();
        }




    }
}
