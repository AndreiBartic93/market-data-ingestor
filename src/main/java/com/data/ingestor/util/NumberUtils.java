package com.data.ingestor.util;

import lombok.experimental.UtilityClass;

import java.math.BigDecimal;

@UtilityClass
public class NumberUtils {

    public static BigDecimal toBigDecimal(String value) {
        return (value != null && !value.isBlank())
                ? new BigDecimal(value)
                : BigDecimal.ZERO;
    }
}
