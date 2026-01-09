package com.data.ingestor.binance.ws;


import com.data.ingestor.config.AppProperties;
import com.data.ingestor.domain.websocket.BinanceWsKlineEvent;
import com.data.ingestor.domain.websocket.CombinedStream;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

@Component
@AllArgsConstructor
public class BinanceWsClient {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String wsBaseUrl;

    private static final TypeReference<CombinedStream<BinanceWsKlineEvent>> TYPE =
            new TypeReference<>() {};

    public Flux<CombinedStream<BinanceWsKlineEvent>> klineCombinedStream(String symbol, List<String> intervals) {
        String streams = intervals.stream()
                .map(i -> symbol.toLowerCase() + "@kline_" + i)
                .collect(Collectors.joining("/"));

        String uri = wsBaseUrl
                + "/stream?streams="
                + URLEncoder.encode(streams, StandardCharsets.UTF_8);

        return httpClient
                .websocket()
                .uri(uri)
                .handle((in, out) -> in.receive().asString())
                .flatMap(json -> Mono.fromCallable(() -> objectMapper.readValue(json, TYPE)));
    }

}
