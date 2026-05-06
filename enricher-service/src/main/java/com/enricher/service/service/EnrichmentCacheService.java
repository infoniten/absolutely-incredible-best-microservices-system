package com.enricher.service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.enricher.service.util.JsonUtils;
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

    public JsonNode get(String key) {
        if (key == null || key.isBlank()) {
            return null;
        }
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null || value.isBlank()) {
                return null;
            }
            return jsonUtils.parseJson(value);
        } catch (Exception ex) {
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
        } catch (Exception ex) {
            log.warn("Failed to write enrichment cache key=[{}]", key, ex);
        }
    }
}
