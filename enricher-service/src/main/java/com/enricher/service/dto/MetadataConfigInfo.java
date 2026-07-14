package com.enricher.service.dto;

import com.enricher.service.domain.MetadataExportV3;

import java.time.OffsetDateTime;

public record MetadataConfigInfo(
        String source,
        String location,
        String hash,
        OffsetDateTime loadedAt,
        MetadataExportV3 config
) {
}
