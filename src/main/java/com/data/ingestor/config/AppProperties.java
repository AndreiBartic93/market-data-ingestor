package com.data.ingestor.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties (
        @Valid Binance binance,
        @Valid Ingestion ingestion,
        @Valid Kafka kafka
) {
    public record Binance(
            @NotBlank String baseUrl
    ) {
        public Binance() { this("https://api.binance.com"); }
    }

    public record Ingestion(
            @NotEmpty List<@NotBlank String> symbols,
            @NotBlank  List<String> intervals,
            @Min(1) int limit,
            @Min(1) int poolSeconds
    ){
        public Ingestion() {
            this(List.of("ETHUSDT"), List.of("1h"), 100, 60);
        }
    }

    public record Kafka(
            @NotBlank String candlesTopic
    ) {
        public Kafka() { this("market.candles.%s"); }
    }
}
