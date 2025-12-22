package com.data.ingestor.domain;

import java.math.BigDecimal;

public record CandleEvent(
        String exchange,          // "BINANCE"
        String symbol,            // "ETHUSDT"
        String interval,          // "1h"
        long openTime,            // millis
        long closeTime,           // millis
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        long eventTime            // millis (cand publicam)
) {}
