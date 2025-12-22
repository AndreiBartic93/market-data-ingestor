package com.data.ingestor.domain;

import java.math.BigDecimal;

public record CandleEvent(
        String exchange,
        String symbol,
        String interval,
        long openTime,
        long closeTime,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        long eventTime
) {}
