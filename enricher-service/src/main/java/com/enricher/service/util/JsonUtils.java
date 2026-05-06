package com.enricher.service.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class JsonUtils {
    private final ObjectMapper objectMapper;

    public JsonUtils(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JsonNode parseJson(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("JSON value is empty");
        }
        try {
            return objectMapper.readTree(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to parse JSON", ex);
        }
    }

    public String toJson(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize JSON", ex);
        }
    }

    public ObjectNode createObjectNode() {
        return objectMapper.createObjectNode();
    }

    public ArrayNode createArrayNode() {
        return objectMapper.createArrayNode();
    }
}
