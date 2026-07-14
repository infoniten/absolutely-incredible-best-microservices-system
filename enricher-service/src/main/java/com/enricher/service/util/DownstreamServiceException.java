package com.enricher.service.util;

public class DownstreamServiceException extends RuntimeException {
    public enum FailureType {
        TIMEOUT,
        UNAVAILABLE,
        RATE_LIMITED,
        BAD_RESPONSE
    }

    private final FailureType failureType;
    private final Integer upstreamStatus;
    private final String retryAfter;

    public DownstreamServiceException(FailureType failureType,
                                      Integer upstreamStatus,
                                      String retryAfter,
                                      String message,
                                      Throwable cause) {
        super(message, cause);
        this.failureType = failureType;
        this.upstreamStatus = upstreamStatus;
        this.retryAfter = retryAfter;
    }

    public FailureType failureType() {
        return failureType;
    }

    public Integer upstreamStatus() {
        return upstreamStatus;
    }

    public String retryAfter() {
        return retryAfter;
    }
}
