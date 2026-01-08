package com.data.ingestor.binance;

import com.data.ingestor.config.AppProperties;
import com.data.ingestor.domain.BinanceKline;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Component
public class BinanceBackfillClient implements BackfillClient{

    private final WebClient webClient;

    public BinanceBackfillClient(AppProperties appProperties) {
        this.webClient = WebClient.builder()
                .baseUrl(appProperties.binance().restBaseUrl())
                .build();
    }

    @Override
    public Mono<List<BinanceKline>> fetchKlines(String symbol, String interval, long startTimeMs, long endTimeMs, int limit) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v3/klines")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
                        .queryParam("startTime", startTimeMs)
                        .queryParam("endTime", endTimeMs)
                        .queryParam("limit", limit)
                        .build()
                )
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<List<Object>>>() {})
                .map(BinanceBackfillClient::toKlines);
    }

    private static List<BinanceKline> toKlines(List<List<Object>> raw) {
        List<BinanceKline> out = new ArrayList<>();
        for (List<Object> r : raw) {
            long openTime = asLong(r.get(0));
            BigDecimal open = asBig(r.get(1));
            BigDecimal high = asBig(r.get(2));
            BigDecimal low  = asBig(r.get(3));
            BigDecimal close= asBig(r.get(4));
            BigDecimal vol  = asBig(r.get(5));
            long closeTime  = asLong(r.get(6));
            out.add(new BinanceKline(openTime, closeTime, open, high, low, close, vol));
        }
        return out;
    }

    //TODO: extract to util
    private static long asLong(Object o) {
        return (o instanceof Number n) ? n.longValue() : Long.parseLong(String.valueOf(o));
    }

    private static BigDecimal asBig(Object o) {
        return new BigDecimal(String.valueOf(o));
    }
}
