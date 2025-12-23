package com.data.ingestor.adapters;

import com.data.ingestor.domain.BinanceKline;
import com.data.ingestor.domain.CandleEvent;
import org.springframework.stereotype.Component;

@Component
public class CandleEventAdapter {
    public CandleEvent fromBinance(String symbol, String interval, BinanceKline kline) {
        long now = System.currentTimeMillis();
        return new CandleEvent(
                "BINANCE",
                symbol,
                interval,
                kline.openTime(),
                kline.closeTime(),
                kline.open(),
                kline.high(),
                kline.low(),
                kline.close(),
                kline.volume(),
                now
        );
    }
}
