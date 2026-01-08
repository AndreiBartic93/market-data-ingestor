package com.data.ingestor.domain.websocket;

import java.math.BigDecimal;

public record BinanceKline (
        long openTime,
        long closeTime,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume
) {}