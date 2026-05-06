package com.enricher.service.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.enricher.service.service.EnrichmentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/enriched-objects")
@RequiredArgsConstructor
@Tag(name = "Enrichment", description = "Enrichment endpoints")
public class EnrichmentController {
    private final EnrichmentService enrichmentService;

    @GetMapping(value = "/{objectClass}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Получить обогащенный объект по globalId",
            description = "Сначала получает объект из search-service по globalId, затем рекурсивно обогащает relation-поля"
    )
    public JsonNode getEnrichedObject(
            @PathVariable String objectClass,
            @RequestParam long globalId,
            @Parameter(
                    description = "Список полей для возврата. Формат source.path, поддержка цепочек relations. Если не передан, возвращается полный JSON объекта",
                    examples = {
                            @ExampleObject(name = "flat", value = "Trade.contractId"),
                            @ExampleObject(name = "nested", value = "Trade.counterparty.name")
                    }
            )
            @RequestParam(name = "outputField", required = false) List<String> outputFields
    ) {
        return enrichmentService.enrich(objectClass, globalId, outputFields);
    }
}
