package com.data.ingestor.domain.websocket;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceWsKlineEvent (String eventType, String symbol, Kline kline) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Kline(
            long openTime, //candle start
            long closeTime, //candle stop
            String interval,
            String open,
            String close,
            String high,
            String low, // OHLC
            String volume, // volume
            boolean isClosed // isClosed: true when candle is final
    ) {}
}
