package com.data.ingestor.binance;

import com.data.ingestor.domain.BinanceKline;
import reactor.core.publisher.Mono;

import java.util.List;

public interface BackfillClient {
    Mono<List<BinanceKline>> fetchKlines(String symbol,
                                         String interval,
                                         long startTimeMs,
                                         long endTimeMs,
                                         int limit);
}
