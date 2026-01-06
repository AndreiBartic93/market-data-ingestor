package com.data.ingestor.domain.websocket;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CombinedStream<T>(String Stream, T data) {}
