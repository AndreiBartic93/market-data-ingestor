package com.data.ingestor.binance;

import com.data.ingestor.domain.websocket.BinanceWsKlineEvent;
import com.data.ingestor.domain.websocket.CandleInterval;
import reactor.core.publisher.Flux;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.stream.Collectors;

public class BinanceKlineWsClient {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String wsBaseUrl;

    public BinanceKlineWsClient(String wsBaseUrl) {
        this.wsBaseUrl = wsBaseUrl;
    }

    public Flux<BinanceWsKlineEvent> streamKlines(List<String> symbols, List<CandleInterval> intervals) {
        String streams = symbols.stream()
                .flatMap(sym -> intervals.stream().map(iv ->
                        sym.toLowerCase() + "@kline_" + iv.binanceId()))
                .collect(Collectors.joining("/"));

        String url = wsBaseUrl + "/stream?streams=" + streams;

        return connectOnce
    }
}
