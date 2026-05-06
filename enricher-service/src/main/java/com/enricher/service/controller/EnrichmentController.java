package com.enricher.service.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.enricher.service.service.EnrichmentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
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
            description = """
                    Сначала получает root-объект из search-service по globalId, затем рекурсивно обогащает relation-поля.
                    Поддерживаемые типы связей: GLOBAL_LINK и EMBEDDED_SET.
                    Если outputField не передан, возвращается полный JSON root-объекта.
                    Если outputField передан, возвращается проекция: root-поля на верхнем уровне, связанные сущности вложенными JSON.
                    """,
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Обогащенный объект",
                            content = @Content(
                                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                                    schema = @Schema(implementation = Object.class),
                                    examples = {
                                            @ExampleObject(
                                                    name = "FullObjectWithoutOutputField",
                                                    value = """
                                                            {
                                                              "objectClass": "FxSpotForwardTrade",
                                                              "id": 12345,
                                                              "contractId": 100500,
                                                              "counterpartyId": 9988
                                                            }
                                                            """
                                            ),
                                            @ExampleObject(
                                                    name = "ProjectionWithNestedRelation",
                                                    value = """
                                                            {
                                                              "objectClass": "FxSpotForwardTrade",
                                                              "id": 12345,
                                                              "counterparty": {
                                                                "id": 9988,
                                                                "name": "ACME BANK"
                                                              }
                                                            }
                                                            """
                                            )
                                    }
                            )
                    ),
                    @ApiResponse(
                            responseCode = "400",
                            description = "Некорректный запрос",
                            content = @Content(
                                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                                    examples = @ExampleObject(
                                            name = "BadRequest",
                                            value = """
                                                    {
                                                      "success": false,
                                                      "status": 400,
                                                      "message": "Invalid outputField selector: [Trade..id], expected source.path",
                                                      "timestamp": "2026-05-06T10:15:30.123+03:00"
                                                    }
                                                    """
                                    )
                            )
                    ),
                    @ApiResponse(
                            responseCode = "404",
                            description = "Объект не найден",
                            content = @Content(
                                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                                    examples = @ExampleObject(
                                            name = "NotFound",
                                            value = """
                                                    {
                                                      "success": false,
                                                      "status": 404,
                                                      "message": "Object not found: objectClass=[FxSpotForwardTrade], globalId=[999999999]",
                                                      "timestamp": "2026-05-06T10:15:30.123+03:00"
                                                    }
                                                    """
                                    )
                            )
                    )
            }
    )
    public JsonNode getEnrichedObject(
            @Parameter(
                    description = "Точный класс объекта (sourceValue или canonical name), например FxSpotForwardTrade",
                    example = "FxSpotForwardTrade"
            )
            @PathVariable String objectClass,
            @Parameter(
                    description = "GlobalId объекта",
                    example = "123"
            )
            @RequestParam long globalId,
            @Parameter(
                    description = "Список полей для возврата. Формат source.path, поддержка цепочек relations. Если не передан, возвращается полный JSON объекта",
                    example = "[\"Trade.contractId\"]",
                    examples = {
                            @ExampleObject(name = "flat", value = "[\"Trade.contractId\"]"),
                            @ExampleObject(name = "nested", value = "[\"Trade.counterparty.name\"]"),
                            @ExampleObject(name = "embeddedSet", value = "[\"Trade.cashflows.amount\"]")
                    }
            )
            @RequestParam(name = "outputField", required = false) List<String> outputFields
    ) {
        return enrichmentService.enrich(objectClass, globalId, outputFields);
    }
}
