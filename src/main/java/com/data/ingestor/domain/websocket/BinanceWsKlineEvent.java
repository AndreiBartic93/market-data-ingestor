package com.data.ingestor.domain.websocket;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceWsKlineEvent (String eventType, String symbol, Kline kline) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Kline(
            long openTime, //t
            long closeTime, //T
            String interval, //i
            String open,
            String close,
            String high,
            String low,
            String volume, //v
            boolean isClosed // x
    ) {}
}
