package com.enricher.service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.enricher.service.config.MetadataProperties;
import com.enricher.service.domain.MetadataExportV2;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

@Component
public class MetadataLoader {
    private final MetadataProperties properties;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;
    private final ResourceLoader resourceLoader;

    public MetadataLoader(MetadataProperties properties,
                          ObjectMapper objectMapper,
                          RestTemplate restTemplate,
                          ResourceLoader resourceLoader) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.restTemplate = restTemplate;
        this.resourceLoader = resourceLoader;
    }

    public MetadataExportV2 load() {
        MetadataExportV2 remote = loadRemote();
        if (remote != null) {
            return remote;
        }
        return loadLocal();
    }

    private MetadataExportV2 loadRemote() {
        String url = properties.url();
        if (url == null || url.isBlank()) {
            return null;
        }
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(URI.create(url), String.class);
            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                return null;
            }
            return validate(objectMapper.readValue(response.getBody(), MetadataExportV2.class));
        } catch (Exception ex) {
            return null;
        }
    }

    private MetadataExportV2 loadLocal() {
        String location = properties.localFile();
        if (location == null || location.isBlank()) {
            throw new IllegalStateException("Metadata configuration is not available: remote failed and local-file is empty");
        }

        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            throw new IllegalStateException("Metadata file not found: [" + location + "]");
        }

        try (InputStream in = resource.getInputStream()) {
            return validate(objectMapper.readValue(in, MetadataExportV2.class));
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load metadata file: [" + location + "]", ex);
        }
    }

    private MetadataExportV2 validate(MetadataExportV2 metadata) {
        if (metadata == null || metadata.classes() == null || metadata.classes().isEmpty()) {
            throw new IllegalStateException("Metadata is empty: classes are required");
        }
        if (metadata.hierarchy() == null || metadata.hierarchy().parentsOrSelf() == null) {
            throw new IllegalStateException("Metadata is invalid: hierarchy.parentsOrSelf is required");
        }
        if (metadata.relations() == null) {
            throw new IllegalStateException("Metadata is invalid: relations are required");
        }
        return metadata;
    }
}
