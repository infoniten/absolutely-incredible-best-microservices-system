package com.enricher.service.dto;

import com.enricher.service.domain.MetadataExportV2;

import java.time.OffsetDateTime;

public record MetadataConfigInfo(
        String source,
        String location,
        String hash,
        OffsetDateTime loadedAt,
        MetadataExportV2 config
) {
}
