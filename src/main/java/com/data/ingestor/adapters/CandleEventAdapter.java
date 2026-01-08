package com.data.ingestor.adapters;

import com.data.ingestor.domain.BinanceKline;
import com.data.ingestor.domain.CandleEvent;
import com.data.ingestor.domain.websocket.BinanceWsKlineEvent;
import com.data.ingestor.domain.websocket.CandleEventWs;
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

    public CandleEventWs fromWs(String symbol, BinanceWsKlineEvent.Kline k) {
        return new CandleEventWs(
                symbol,
                k.interval(),
                k.openTime(),
                k.closeTime(),
                k.open(),
                k.high(),
                k.low(),
                k.close(),
                k.volume(),
                k.isClosed(),
                k.isClosed() ? "CLOSE" : "LIVE"
        );
    }

    public CandleEventWs fromBackfill(String symbol, String interval, com.data.ingestor.domain.websocket.BinanceKline k) {
        return new CandleEventWs(
                symbol,
                interval,
                k.openTime(),
                k.closeTime(),
                k.open().toPlainString(),
                k.high().toPlainString(),
                k.low().toPlainString(),
                k.close().toPlainString(),
                k.volume().toPlainString(),
                true,
                "BACKFILL"
        );
    }
}
