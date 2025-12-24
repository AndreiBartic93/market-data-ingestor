package com.data.ingestor.binance;

import com.data.ingestor.domain.BinanceKline;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Component
public class BinanceRestClient {
    private final WebClient webClient;

    public BinanceRestClient(@Value("${app.binance.base-url}") String baseUrl) {
        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .build();
    }

    public Mono<List<BinanceKline>> fetchKlines(String symbol, String interval, int limit) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v3/klines")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
                        .queryParam("limit", limit)
                        .build()
                )
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<List<Object>>>() {})
                .map(BinanceRestClient::toKlines);
    }

    private static List<BinanceKline> toKlines(List<List<Object>> raw) {
        List<BinanceKline> out = new ArrayList<>(raw.size());

        for (List<Object> r : raw) {
            long openTime = asLong(r.get(0));
            BigDecimal open = asBigDecimal(r.get(1));
            BigDecimal high = asBigDecimal(r.get(2));
            BigDecimal low  = asBigDecimal(r.get(3));
            BigDecimal close = asBigDecimal(r.get(4));
            BigDecimal volume = asBigDecimal(r.get(5));
            long closeTime = asLong(r.get(6));

            out.add(new BinanceKline(openTime, open, high, low, close, volume, closeTime));
        }

        return out;
    }

    private static long asLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(v));
    }

    private static BigDecimal asBigDecimal(Object v) {
        return new BigDecimal(String.valueOf(v));
    }
}
