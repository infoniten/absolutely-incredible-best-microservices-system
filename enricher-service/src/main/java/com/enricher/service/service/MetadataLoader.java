package com.enricher.service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.enricher.service.config.MetadataProperties;
import com.enricher.service.domain.MetadataExportV2;
import com.enricher.service.dto.MetadataConfigInfo;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Component
public class MetadataLoader {
    private final MetadataProperties properties;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;
    private final ResourceLoader resourceLoader;
    private volatile MetadataConfigInfo info;

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
        LoadedMetadata remote = loadRemote();
        if (remote != null) {
            MetadataConfigInfo configInfo = buildInfo(remote.source(), remote.location(), remote.config());
            info = configInfo;
            return remote.config();
        }
        LoadedMetadata local = loadLocal();
        MetadataConfigInfo configInfo = buildInfo(local.source(), local.location(), local.config());
        info = configInfo;
        return local.config();
    }

    public MetadataConfigInfo info() {
        MetadataConfigInfo current = info;
        if (current == null) {
            throw new IllegalStateException("Metadata config is not loaded");
        }
        return current;
    }

    private LoadedMetadata loadRemote() {
        String url = properties.url();
        if (url == null || url.isBlank()) {
            return null;
        }
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(URI.create(url), String.class);
            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null || response.getBody().isBlank()) {
                return null;
            }
            MetadataExportV2 config = validate(objectMapper.readValue(response.getBody(), MetadataExportV2.class));
            return new LoadedMetadata("remote", url, config);
        } catch (Exception ex) {
            return null;
        }
    }

    private LoadedMetadata loadLocal() {
        String location = properties.localFile();
        if (location == null || location.isBlank()) {
            throw new IllegalStateException("Metadata configuration is not available: remote failed and local-file is empty");
        }

        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            throw new IllegalStateException("Metadata file not found: [" + location + "]");
        }

        try (InputStream in = resource.getInputStream()) {
            MetadataExportV2 config = validate(objectMapper.readValue(in, MetadataExportV2.class));
            return new LoadedMetadata("local", location, config);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load metadata file: [" + location + "]", ex);
        }
    }

    private MetadataConfigInfo buildInfo(String source, String location, MetadataExportV2 config) {
        String canonical = canonicalJson(config);
        String hash = sha256Hex(canonical);
        OffsetDateTime loadedAt = OffsetDateTime.now(ZoneOffset.UTC);
        return new MetadataConfigInfo(source, location, hash, loadedAt, config);
    }

    private String canonicalJson(MetadataExportV2 config) {
        try {
            ObjectMapper mapper = objectMapper.copy();
            mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
            mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
            mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
            return mapper.writeValueAsString(config);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize metadata config", ex);
        }
    }

    private String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(hashed.length * 2);
            for (byte b : hashed) {
                out.append(String.format("%02x", b & 0xff));
            }
            return out.toString();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to compute metadata config hash", ex);
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

    private record LoadedMetadata(String source, String location, MetadataExportV2 config) {
    }
}
