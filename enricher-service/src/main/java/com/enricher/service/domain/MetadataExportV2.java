package com.enricher.service.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MetadataExportV2(
        List<ClassConfig> classes,
        Map<String, FieldsConfig> fields,
        HierarchyConfig hierarchy,
        Map<String, List<RelationConfig>> relations
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ClassConfig(
            String name,
            String sourceValue,
            boolean isAbstract,
            String rootType,
            String mainTable,
            String dataTable
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldsConfig(
            List<FieldConfig> mainFields,
            List<FieldConfig> declaredFields
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldConfig(
            String name,
            String db,
            String type
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HierarchyConfig(
            Map<String, List<String>> parentsOrSelf,
            Map<String, String> rootUnderBase,
            Map<String, List<String>> actualClassesForRoot
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RelationConfig(
            String name,
            String alias,
            String type,
            String targetClass,
            String sourceFieldName,
            String sourceJsonName,
            String idFieldName,
            String jsonIdFieldName,
            String roleFieldName,
            String jsonRoleFieldName,
            String roleValue,
            String sourceFieldDeclaredInClass,
            String idFieldDeclaredInClass,
            String roleFieldDeclaredInClass
    ) {
    }
}
