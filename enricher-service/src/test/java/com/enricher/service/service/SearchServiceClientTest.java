package com.enricher.service.service;

import com.enricher.service.config.SearchServiceClientProperties;
import com.enricher.service.controller.ApiExceptionHandler;
import com.enricher.service.dto.ResultStructure;
import com.enricher.service.util.DownstreamServiceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class SearchServiceClientTest {
    private MockRestServiceServer server;
    private SearchServiceClient client;

    @BeforeEach
    void setUp() {
        SearchServiceClientProperties properties = new SearchServiceClientProperties();
        properties.setBaseUrl("http://search-service");
        properties.setGlobalEndpoint("/api/objects/{objectClass}?globalId={globalId}");
        properties.setRevisionEndpoint("/api/objects/{objectClass}/revisions");
        properties.setParentEndpoint("/api/objects/{objectClass}/parent/{parentId}");
        properties.setGlobalItemEndpoint("/api/objects/{objectClass}/global-item?relationGlobalId={relationGlobalId}&idFieldName={idFieldName}&roleFieldName={roleFieldName}&role={role}");

        RestTemplate restTemplate = new RestTemplate();
        server = MockRestServiceServer.bindTo(restTemplate).build();
        client = new SearchServiceClient(
                properties,
                restTemplate,
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    @Test
    void mapsUpstreamServiceUnavailableToTypedException() {
        server.expect(requestTo("http://search-service/api/objects/Trade?globalId=1"))
                .andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        assertThatThrownBy(() -> client.getObjectByGlobalId("Trade", 1L))
                .isInstanceOfSatisfying(DownstreamServiceException.class, ex -> {
                    assertThat(ex.failureType()).isEqualTo(DownstreamServiceException.FailureType.UNAVAILABLE);
                    assertThat(ex.upstreamStatus()).isEqualTo(503);
                });
        server.verify();
    }

    @Test
    void propagatesRateLimitAndRetryAfterThroughApiHandler() {
        server.expect(requestTo("http://search-service/api/objects/Trade?globalId=1"))
                .andRespond(withStatus(HttpStatus.TOO_MANY_REQUESTS)
                        .header(HttpHeaders.RETRY_AFTER, "30"));

        DownstreamServiceException exception = org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> client.getObjectByGlobalId("Trade", 1L),
                DownstreamServiceException.class
        );
        ResponseEntity<ResultStructure> response = new ApiExceptionHandler().handleDownstreamFailure(exception);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("30");
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo(429);
        server.verify();
    }

    @Test
    void callsRevisionBatchEndpointWithoutIdPathVariable() {
        server.expect(requestTo("http://search-service/api/objects/Trade/revisions"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON));

        JsonNode result = client.getObjectRevisionByIds("Trade", List.of(1L, 2L));

        assertThat(result.isArray()).isTrue();
        server.verify();
    }
}
