package com.enricher.service.controller;

import com.enricher.service.dto.MetadataConfigInfo;
import com.enricher.service.service.MetadataLoader;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/config")
@RequiredArgsConstructor
@Tag(name = "Config", description = "Configuration endpoints")
public class ConfigController {
    private final MetadataLoader metadataLoader;

    @GetMapping(value = "/domain", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Получить активный metadata-config", description = "Возвращает source, location, hash, loadedAt и текущее тело metadata-config")
    public MetadataConfigInfo getDomainConfig() {
        return metadataLoader.info();
    }
}
