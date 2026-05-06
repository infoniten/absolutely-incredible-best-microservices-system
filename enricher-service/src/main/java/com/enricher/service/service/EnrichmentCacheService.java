package com.enricher.service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.enricher.service.util.JsonUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EnrichmentCacheService {
    private final StringRedisTemplate stringRedisTemplate;
    private final JsonUtils jsonUtils;
    private final MeterRegistry meterRegistry;

    private static final String CACHE_GET_HIT = "enricher.cache.get.hit.count";
    private static final String CACHE_GET_MISS = "enricher.cache.get.miss.count";
    private static final String CACHE_GET_ERROR = "enricher.cache.get.error.count";
    private static final String CACHE_PUT_SUCCESS = "enricher.cache.put.success.count";
    private static final String CACHE_PUT_ERROR = "enricher.cache.put.error.count";

    public JsonNode get(String key) {
        if (key == null || key.isBlank()) {
            meterRegistry.counter(CACHE_GET_MISS).increment();
            return null;
        }
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null || value.isBlank()) {
                meterRegistry.counter(CACHE_GET_MISS).increment();
                return null;
            }
            JsonNode parsed = jsonUtils.parseJson(value);
            meterRegistry.counter(CACHE_GET_HIT).increment();
            return parsed;
        } catch (Exception ex) {
            meterRegistry.counter(CACHE_GET_ERROR).increment();
            log.warn("Failed to read enrichment cache key=[{}]", key, ex);
            return null;
        }
    }

    public void put(String key, JsonNode value) {
        if (key == null || key.isBlank() || value == null) {
            return;
        }
        try {
            stringRedisTemplate.opsForValue().set(key, jsonUtils.toJson(value));
            meterRegistry.counter(CACHE_PUT_SUCCESS).increment();
        } catch (Exception ex) {
            meterRegistry.counter(CACHE_PUT_ERROR).increment();
            log.warn("Failed to write enrichment cache key=[{}]", key, ex);
        }
    }
}
