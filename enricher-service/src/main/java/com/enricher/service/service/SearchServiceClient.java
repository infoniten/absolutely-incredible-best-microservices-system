package com.enricher.service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enricher.service.config.SearchServiceClientProperties;
import com.enricher.service.util.NotFoundException;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

@Component
@Slf4j
public class SearchServiceClient {
    private record RequestMetricNames(String count, String errors, String duration) {
    }

    private final SearchServiceClientProperties properties;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final RequestMetricNames globalMetrics;
    private final RequestMetricNames revisionMetrics;
    private final RequestMetricNames parentMetrics;

    public SearchServiceClient(SearchServiceClientProperties properties,
                               RestTemplate restTemplate,
                               ObjectMapper objectMapper,
                               MeterRegistry meterRegistry) {
        this.properties = properties;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.globalMetrics = metricNames("enricher.search.global");
        this.revisionMetrics = metricNames("enricher.search.revision");
        this.parentMetrics = metricNames("enricher.search.parent");
    }

    public JsonNode getObjectByGlobalId(String objectClass, long globalId) {
        log.info("Search-service global request: objectClass=[{}], globalId=[{}]", objectClass, globalId);
        return recordRequest(
                globalMetrics,
                () -> {
                    URI uri = buildUri(
                            globalEndpoint(),
                            Map.of(
                                    "objectClass", objectClass,
                                    "globalId", globalId
                            )
                    );
                    try {
                        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
                        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                            throw new NotFoundException("Object not found: objectClass=[" + objectClass + "], globalId=[" + globalId + "]");
                        }
                        log.info("Search-service global response: objectClass=[{}], globalId=[{}], status=[{}]",
                                objectClass, globalId, response.getStatusCode().value());
                        return objectMapper.readTree(response.getBody());
                    } catch (HttpClientErrorException.NotFound ex) {
                        log.info("Search-service global response not found: objectClass=[{}], globalId=[{}]",
                                objectClass, globalId);
                        throw new NotFoundException("Object not found: objectClass=[" + objectClass + "], globalId=[" + globalId + "]");
                    } catch (NotFoundException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        log.warn("Search-service global request failed: objectClass=[{}], globalId=[{}]",
                                objectClass, globalId, ex);
                        throw new IllegalStateException("Failed to call search-service getObjectByGlobalId", ex);
                    }
                },
                "object_class", normalizeTag(objectClass)
        );
    }

    public JsonNode getObjectCollectionByParentId(String objectClass, long parentId) {
        log.info("Search-service parent request: objectClass=[{}], parentId=[{}]", objectClass, parentId);
        return recordRequest(
                parentMetrics,
                () -> {
                    URI uri = buildUri(
                            parentEndpoint(),
                            Map.of(
                                    "objectClass", objectClass,
                                    "parentId", parentId
                            )
                    );
                    try {
                        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
                        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                            log.info("Search-service parent response is empty: objectClass=[{}], parentId=[{}], status=[{}]",
                                    objectClass, parentId, response.getStatusCode().value());
                            return objectMapper.createArrayNode();
                        }
                        JsonNode parsed = objectMapper.readTree(response.getBody());
                        log.info("Search-service parent response: objectClass=[{}], parentId=[{}], count=[{}]",
                                objectClass, parentId, parsed.isArray() ? parsed.size() : 0);
                        return parsed;
                    } catch (HttpClientErrorException.NotFound ex) {
                        log.info("Search-service parent response not found: objectClass=[{}], parentId=[{}]",
                                objectClass, parentId);
                        return objectMapper.createArrayNode();
                    } catch (Exception ex) {
                        log.warn("Search-service parent request failed: objectClass=[{}], parentId=[{}]",
                                objectClass, parentId, ex);
                        throw new IllegalStateException("Failed to call search-service getObjectCollectionByParentId", ex);
                    }
                },
                "object_class", normalizeTag(objectClass)
        );
    }

    public JsonNode getObjectRevisionById(String objectClass, long id) {
        log.info("Search-service revision request: objectClass=[{}], id=[{}]", objectClass, id);
        return recordRequest(
                revisionMetrics,
                () -> {
                    URI uri = buildUri(
                            revisionEndpoint(),
                            Map.of(
                                    "objectClass", objectClass,
                                    "id", id
                            )
                    );
                    try {
                        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
                        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                            throw new NotFoundException("Object revision not found: objectClass=[" + objectClass + "], id=[" + id + "]");
                        }
                        log.info("Search-service revision response: objectClass=[{}], id=[{}], status=[{}]",
                                objectClass, id, response.getStatusCode().value());
                        return objectMapper.readTree(response.getBody());
                    } catch (HttpClientErrorException.NotFound ex) {
                        log.info("Search-service revision response not found: objectClass=[{}], id=[{}]",
                                objectClass, id);
                        throw new NotFoundException("Object revision not found: objectClass=[" + objectClass + "], id=[" + id + "]");
                    } catch (NotFoundException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        log.warn("Search-service revision request failed: objectClass=[{}], id=[{}]",
                                objectClass, id, ex);
                        throw new IllegalStateException("Failed to call search-service getObjectRevisionById", ex);
                    }
                },
                "object_class", normalizeTag(objectClass)
        );
    }

    private <T> T recordRequest(RequestMetricNames metrics, Supplier<T> action, String... tags) {
        if ((tags.length & 1) == 1) {
            throw new IllegalArgumentException("Metric tags must be key-value pairs");
        }
        meterRegistry.counter(metrics.count(), tags).increment();
        try {
            return meterRegistry.timer(metrics.duration(), tags).record(action::get);
        } catch (RuntimeException ex) {
            meterRegistry.counter(metrics.errors(), tags).increment();
            throw ex;
        }
    }

    private static RequestMetricNames metricNames(String metricPrefix) {
        return new RequestMetricNames(
                metricPrefix + ".count",
                metricPrefix + ".errors",
                metricPrefix + ".duration"
        );
    }

    private String normalizeTag(String value) {
        if (value == null || value.isBlank()) {
            return "none";
        }
        String source = value.trim().toLowerCase(Locale.ROOT);
        StringBuilder out = new StringBuilder(source.length());
        for (int i = 0; i < source.length(); i++) {
            char ch = source.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.') {
                out.append(ch);
            } else {
                out.append('_');
            }
        }
        return out.toString();
    }

    private String baseUrl() {
        String baseUrl = properties.baseUrl();
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalStateException("enricher.search-service.base-url is not configured");
        }
        return baseUrl;
    }

    private String globalEndpoint() {
        String endpoint = properties.globalEndpoint();
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("enricher.search-service.global-endpoint is not configured");
        }
        return endpoint;
    }

    private String parentEndpoint() {
        String endpoint = properties.parentEndpoint();
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("enricher.search-service.parent-endpoint is not configured");
        }
        return endpoint;
    }

    private String revisionEndpoint() {
        String endpoint = properties.revisionEndpoint();
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("enricher.search-service.revision-endpoint is not configured");
        }
        return endpoint;
    }

    private URI buildUri(String endpointTemplate, Map<String, Object> uriVariables) {
        return UriComponentsBuilder
                .fromUriString(baseUrl() + endpointTemplate)
                .buildAndExpand(uriVariables)
                .toUri();
    }
}
