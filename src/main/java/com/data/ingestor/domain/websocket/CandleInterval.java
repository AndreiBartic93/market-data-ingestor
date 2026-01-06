package com.data.ingestor.domain.websocket;

public enum CandleInterval {

    H1("h1", 60 * 60 * 1000L),
    H4("h4", 4 * 60 * 60 * 1000L);

    private final String binanceId;
    private final long ms;

    CandleInterval(String binanceId, long ms) {
        this.binanceId = binanceId;
        this.ms = ms;
    }

    public String binanceId() { return binanceId; }
    public long ms() { return ms; }

    public static CandleInterval from(String v) {
        return switch(v.toLowerCase()) {
            case "1h" -> H1;
            case "4h" -> H4;
            default -> throw new IllegalArgumentException("Unsupported interval: " + v);
        };
    }
}
