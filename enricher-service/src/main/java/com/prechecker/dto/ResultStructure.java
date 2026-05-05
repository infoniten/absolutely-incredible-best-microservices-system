package com.prechecker.dto;

import java.time.OffsetDateTime;

public record ResultStructure(
        boolean success,
        int status,
        String message,
        String timestamp
) {
    public static ResultStructure error(int status, String message) {
        return new ResultStructure(false, status, message, OffsetDateTime.now().toString());
    }
}
