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
    ) {}

    public record Ingestion(
            @NotEmpty List<@NotBlank String> symbols,
            @NotBlank String interval,
            @Min(1) int limit,
            @Min(1) int poolSeconds
    ){}

    public record Kafka(
            @NotBlank String candlesTopic
    ) {}
}
