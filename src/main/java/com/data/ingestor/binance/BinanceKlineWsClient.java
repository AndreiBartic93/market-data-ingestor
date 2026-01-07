package com.data.ingestor.binance;

import com.data.ingestor.domain.websocket.BinanceWsKlineEvent;
import com.data.ingestor.domain.websocket.CandleInterval;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class BinanceKlineWsClient {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String wsBaseUrl;

    public BinanceKlineWsClient(String wsBaseUrl) {
        this.wsBaseUrl = wsBaseUrl;
    }

    //Using Netty reactor HttpClient to obtain Flux<String> of messages then parse with Jackson
    public Flux<BinanceWsKlineEvent> streamKlines(List<String> symbols, List<CandleInterval> intervals) {
        String streams = symbols.stream()
                .flatMap(sym -> intervals.stream().map(iv ->
                        sym.toLowerCase() + "@kline_" + iv.binanceId()))
                .collect(Collectors.joining("/"));

        String url = wsBaseUrl + "/stream?streams=" + streams;

        return connectOnce(url)
                .map(this::parseKlineEvent)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .jitter(0.2)
                );
    }

    private Flux<String> connectOnce(String url) {
        return Flux.create(sink -> {
            HttpClient.create()
                    .websocket()
                    .uri(url)
                    .handle((in, out) -> in.receive()
                            .asString()
                            .doOnNext(sink::next)
                            .doOnError(sink::error)
                            .doOnComplete(sink::complete)
                            .then()
                    )
                    .subscribe();
        });
    }

    private BinanceWsKlineEvent parseKlineEvent(String json) {
        try {
            JsonNode root = mapper.readTree(json);
            JsonNode dataNode = root.has("data") ? root.get("data") : root;
            return mapper.treeToValue(dataNode, BinanceWsKlineEvent.class);
        } catch(Exception e) {
            throw new RuntimeException("Failed to parse WS message: " + json, e);
        }
    }
}
