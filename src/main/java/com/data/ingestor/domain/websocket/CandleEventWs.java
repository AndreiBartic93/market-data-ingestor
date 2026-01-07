package com.data.ingestor.domain.websocket;

public record CandleEventWs(
        String symbol,
        String interval,
        long openTime,
        long closeTime,
        String open,
        String high,
        String low,
        String close,
        String volume,
        boolean closed,
        String eventType // "LIVE" | "CLOSE" | "BACKFILL"
)


