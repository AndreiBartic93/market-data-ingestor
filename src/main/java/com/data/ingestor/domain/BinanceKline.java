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
) {
    public static BinanceKline fromArray(Object[] a) {
        return new BinanceKline(
                ((Number) a[0]).longValue(),
                new BigDecimal(a[1].toString()),
                new BigDecimal(a[2].toString()),
                new BigDecimal(a[3].toString()),
                new BigDecimal(a[4].toString()),
                new BigDecimal(a[5].toString()),
                ((Number) a[6]).longValue()
        );
    }
}
