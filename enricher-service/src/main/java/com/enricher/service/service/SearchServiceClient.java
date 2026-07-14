package com.enricher.service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enricher.service.config.SearchServiceClientProperties;
import com.enricher.service.util.DownstreamServiceException;
import com.enricher.service.util.NotFoundException;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

@Component
@Slf4j
public class SearchServiceClient {
    // TODO: Добавить ограниченные повторы при временных сбоях search-service только для безопасных
    // идемпотентных операций, с экспоненциальной задержкой и jitter в пределах таймаута запроса.
    private record RequestMetricNames(String count, String errors, String duration) {
    }

    private final SearchServiceClientProperties properties;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final RequestMetricNames globalMetrics;
    private final RequestMetricNames globalItemMetrics;
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
        this.globalItemMetrics = metricNames("enricher.search.global_item");
        this.revisionMetrics = metricNames("enricher.search.revision");
        this.parentMetrics = metricNames("enricher.search.parent");
    }

    public JsonNode getObjectByGlobalId(String objectClass, long globalId) {
        log.info("Search-service global request: objectClass=[{}], globalId=[{}]", objectClass, globalId);
        return recordRequest(
                globalMetrics,
                () -> {
                    try {
                        URI uri = buildUri(
                                globalEndpoint(),
                                Map.of(
                                        "objectClass", objectClass,
                                        "globalId", globalId
                                )
                        );
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
                        throw translateFailure("getObjectByGlobalId", ex);
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
                    try {
                        URI uri = buildUri(
                                parentEndpoint(),
                                Map.of(
                                        "objectClass", objectClass,
                                        "parentId", parentId
                                )
                        );
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
                        throw translateFailure("getObjectCollectionByParentId", ex);
                    }
                },
                "object_class", normalizeTag(objectClass)
        );
    }

    public JsonNode getObjectByGlobalItem(String objectClass,
                                          long relationGlobalId,
                                          String idFieldName,
                                          String roleFieldName,
                                          String role) {
        requireNonBlank(objectClass, "objectClass");
        requireNonBlank(idFieldName, "idFieldName");
        requireNonBlank(roleFieldName, "roleFieldName");
        requireNonBlank(role, "role");

        log.info("Search-service global-item request: objectClass=[{}], relationGlobalId=[{}], idFieldName=[{}], roleFieldName=[{}], role=[{}]",
                objectClass, relationGlobalId, idFieldName, roleFieldName, role);
        return recordRequest(
                globalItemMetrics,
                () -> {
                    try {
                        URI uri = buildUri(
                                globalItemEndpoint(),
                                Map.of(
                                        "objectClass", objectClass,
                                        "relationGlobalId", relationGlobalId,
                                        "idFieldName", idFieldName,
                                        "roleFieldName", roleFieldName,
                                        "role", role
                                )
                        );
                        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
                        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                            throw new NotFoundException("Global item object not found: relationGlobalId=[" + relationGlobalId
                                    + "], idFieldName=[" + idFieldName + "], roleFieldName=[" + roleFieldName + "], role=[" + role + "]");
                        }
                        log.info("Search-service global-item response: objectClass=[{}], relationGlobalId=[{}], idFieldName=[{}], roleFieldName=[{}], role=[{}], status=[{}]",
                                objectClass, relationGlobalId, idFieldName, roleFieldName, role, response.getStatusCode().value());
                        return objectMapper.readTree(response.getBody());
                    } catch (HttpClientErrorException.NotFound ex) {
                        log.info("Search-service global-item response not found: objectClass=[{}], relationGlobalId=[{}], idFieldName=[{}], roleFieldName=[{}], role=[{}]",
                                objectClass, relationGlobalId, idFieldName, roleFieldName, role);
                        throw new NotFoundException("Global item object not found: relationGlobalId=[" + relationGlobalId
                                + "], idFieldName=[" + idFieldName + "], roleFieldName=[" + roleFieldName + "], role=[" + role + "]");
                    } catch (NotFoundException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        log.warn("Search-service global-item request failed: objectClass=[{}], relationGlobalId=[{}], idFieldName=[{}], roleFieldName=[{}], role=[{}]",
                                objectClass, relationGlobalId, idFieldName, roleFieldName, role, ex);
                        throw translateFailure("getObjectByGlobalItem", ex);
                    }
                },
                "object_class", normalizeTag(objectClass),
                "id_field", normalizeTag(idFieldName),
                "role_field", normalizeTag(roleFieldName),
                "role", normalizeTag(role)
        );
    }

    public JsonNode getObjectRevisionByIds(String objectClass, List<Long> ids) {
        log.info("Search-service revision request: objectClass=[{}], id=[{}]", objectClass, ids);
        return recordRequest(
                revisionMetrics,
                () -> {
                    try {
                        URI uri = buildUri(
                                revisionEndpoint(),
                                Map.of(
                                        "objectClass", objectClass
                                )
                        );
                        ResponseEntity<String> response = restTemplate.postForEntity(uri, ids, String.class);
                        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                            log.info("Search-service revision response is empty: objectClass=[{}], ids=[{}], status=[{}]", objectClass, ids, response.getStatusCode().value());
                            return objectMapper.createArrayNode();
                        }
                        JsonNode parsed = objectMapper.readTree(response.getBody());
                        log.info("Search-service revision response: objectClass=[{}], ids=[{}], count=[{}], status=[{}]",
                                objectClass, ids, parsed.isArray() ? parsed.size(): 0, response.getStatusCode().value());
                        return parsed;
                    } catch (HttpClientErrorException.NotFound ex) {
                        log.info("Search-service revision response not found: objectClass=[{}], ids=[{}]",
                                objectClass, ids);
                        return objectMapper.createArrayNode();
                    }  catch (Exception ex) {
                        log.warn("Search-service revision request failed: objectClass=[{}], ids=[{}]",
                                objectClass, ids, ex);
                        throw translateFailure("getObjectRevisionByIds", ex);
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

    private DownstreamServiceException translateFailure(String operation, Exception ex) {
        if (ex instanceof DownstreamServiceException downstreamException) {
            return downstreamException;
        }
        if (ex instanceof ResourceAccessException) {
            DownstreamServiceException.FailureType type = hasCause(ex, SocketTimeoutException.class)
                    ? DownstreamServiceException.FailureType.TIMEOUT
                    : DownstreamServiceException.FailureType.UNAVAILABLE;
            return new DownstreamServiceException(
                    type,
                    null,
                    null,
                    "Search-service request failed: operation=[" + operation + "]",
                    ex
            );
        }
        if (ex instanceof RestClientResponseException responseException) {
            int upstreamStatus = responseException.getStatusCode().value();
            String retryAfter = responseException.getResponseHeaders() == null
                    ? null
                    : responseException.getResponseHeaders().getFirst("Retry-After");
            DownstreamServiceException.FailureType type;
            if (upstreamStatus == 429) {
                type = DownstreamServiceException.FailureType.RATE_LIMITED;
            } else if (upstreamStatus == 503) {
                type = DownstreamServiceException.FailureType.UNAVAILABLE;
            } else if (upstreamStatus == 504) {
                type = DownstreamServiceException.FailureType.TIMEOUT;
            } else {
                type = DownstreamServiceException.FailureType.BAD_RESPONSE;
            }
            return new DownstreamServiceException(
                    type,
                    upstreamStatus,
                    retryAfter,
                    "Search-service returned an error: operation=[" + operation + "], status=[" + upstreamStatus + "]",
                    ex
            );
        }
        return new DownstreamServiceException(
                DownstreamServiceException.FailureType.BAD_RESPONSE,
                null,
                null,
                "Invalid response from search-service: operation=[" + operation + "]",
                ex
        );
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> causeType) {
        Throwable current = throwable;
        while (current != null) {
            if (causeType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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

    private String globalItemEndpoint() {
        String endpoint = properties.globalItemEndpoint();
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("enricher.search-service.global-item-endpoint is not configured");
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

    private void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Search-service global-item request requires " + name);
        }
    }
}
