package com.data.ingestor.domain;

import java.math.BigDecimal;

public record BinanceKline (
        long openTime,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        long closeTime
) {}
