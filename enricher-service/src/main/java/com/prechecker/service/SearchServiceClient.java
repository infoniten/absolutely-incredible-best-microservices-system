package com.prechecker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prechecker.config.SearchServiceClientProperties;
import com.prechecker.util.NotFoundException;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

@Component
public class SearchServiceClient {
    private final SearchServiceClientProperties properties;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public SearchServiceClient(SearchServiceClientProperties properties,
                               RestTemplate restTemplate,
                               ObjectMapper objectMapper) {
        this.properties = properties;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public JsonNode getObjectByGlobalId(String objectClass, long globalId) {
        URI uri = UriComponentsBuilder.fromHttpUrl(baseUrl())
                .pathSegment("api", "objects", objectClass)
                .queryParam("globalId", globalId)
                .build()
                .toUri();
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                throw new NotFoundException("Object not found: objectClass=[" + objectClass + "], globalId=[" + globalId + "]");
            }
            return objectMapper.readTree(response.getBody());
        } catch (HttpClientErrorException.NotFound ex) {
            throw new NotFoundException("Object not found: objectClass=[" + objectClass + "], globalId=[" + globalId + "]");
        } catch (NotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to call search-service getObjectByGlobalId", ex);
        }
    }

    public JsonNode getObjectCollectionByParentId(String objectClass, long parentId) {
        URI uri = UriComponentsBuilder.fromHttpUrl(baseUrl())
                .pathSegment("api", "objects", objectClass, "parent", String.valueOf(parentId))
                .build()
                .toUri();
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                return objectMapper.createArrayNode();
            }
            return objectMapper.readTree(response.getBody());
        } catch (HttpClientErrorException.NotFound ex) {
            return objectMapper.createArrayNode();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to call search-service getObjectCollectionByParentId", ex);
        }
    }

    private String baseUrl() {
        String baseUrl = properties.baseUrl();
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalStateException("enricher.search-service.base-url is not configured");
        }
        return baseUrl;
    }
}
