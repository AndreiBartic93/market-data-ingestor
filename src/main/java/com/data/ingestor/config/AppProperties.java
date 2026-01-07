package com.data.ingestor.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties (
        @Valid @NotNull Binance binance,
        @Valid @NotNull Ingestion ingestion,
        @Valid @NotNull Kafka kafka,
        @Valid @NotNull Ws ws
) {
    public record Binance(
            @NotBlank String restBaseUrl,
            @NotBlank String wsBaseUrl
    ) {
    }

    public record Ingestion(
            @NotEmpty List<@NotBlank String> symbols,
            @NotEmpty List<@NotBlank String> intervals,
            @Min(1) int limit,
            @Min(1) int pollSeconds,
            @Min(0) int bootstrapLimit
    ){
    }

    public record Kafka(
            @NotBlank String candlesTopicPrefix,
            @NotBlank boolean separateLiveTopic
    ) {
    }

    public record Ws(
            @Min(1) int reconnectBackoffSeconds,
            @Min(1) int maxReconnectBackoffSeconds
    ) {}
}
