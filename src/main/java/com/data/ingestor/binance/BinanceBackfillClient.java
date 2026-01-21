package com.data.ingestor.binance;

import com.data.ingestor.config.AppProperties;


import com.data.ingestor.domain.websocket.BinanceKline;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.data.ingestor.util.NumberUtils.toBigDecimal;

@Component
public class BinanceBackfillClient implements BackfillClient {

    private final WebClient webClient;
    private static final int LIMIT = 1000;

    public BinanceBackfillClient(AppProperties props) {
        this.webClient = WebClient.builder()
                .baseUrl(props.binance().restBaseUrl())
                .build();
    }

    @Override
    public Mono<List<BinanceKline>> fetchKlinesRange(String symbol, String interval, long startTimeMs, long endTimeMs) {
        if (endTimeMs <= startTimeMs) return Mono.just(List.of());

        return fetchPage(symbol, interval, startTimeMs, endTimeMs)
                .expand(page -> {
                    if (page.isEmpty()) return Mono.empty();
                    long lastOpen = page.get(page.size() - 1).openTime();
                    long nextStart = lastOpen + 1; // evită duplicate
                    if (nextStart >= endTimeMs) return Mono.empty();
                    return fetchPage(symbol, interval, nextStart, endTimeMs);
                })
                .reduce(new ArrayList<BinanceKline>(), (acc, page) -> { acc.addAll(page); return acc; })
                .map(list -> {
                    list.sort(Comparator.comparingLong(BinanceKline::openTime));
                    // dedup by openTime
                    List<BinanceKline> out = new ArrayList<>(list.size());
                    Long prev = null;
                    for (var k : list) {
                        if (prev == null || k.openTime() != prev) out.add(k);
                        prev = k.openTime();
                    }
                    return out;
                });
    }

    private Mono<List<BinanceKline>> fetchPage(String symbol, String interval, long startTimeMs, long endTimeMs) {
        return webClient.get()
                .uri(uri -> uri.path("/api/v3/klines")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
                        .queryParam("startTime", startTimeMs)
                        .queryParam("endTime", endTimeMs)
                        .queryParam("limit", LIMIT)
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<List<Object>>>() {})
                .map(BinanceBackfillClient::toKlines);
    }

    private static List<BinanceKline> toKlines(List<List<Object>> raw) {
        List<BinanceKline> out = new ArrayList<>(raw.size());
        for (var r : raw) {
            long openTime  = asLong(r.get(0));
            String open    = String.valueOf(r.get(1));
            String high    = String.valueOf(r.get(2));
            String low     = String.valueOf(r.get(3));
            String close   = String.valueOf(r.get(4));
            String volume  = String.valueOf(r.get(5));
            long closeTime = asLong(r.get(6));
            out.add(new BinanceKline(
                    openTime,
                    closeTime,
                    toBigDecimal(open),
                    toBigDecimal(high),
                    toBigDecimal(low),
                    toBigDecimal(close),
                    toBigDecimal(volume)
            ));
        }
        return out;
    }

    private static long asLong(Object o) {
        return (o instanceof Number n) ? n.longValue() : Long.parseLong(String.valueOf(o));
    }


}
