package com.enricher.service.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MetadataExportV3(
        List<ClassConfig> classes,
        Map<String, FieldsConfig> fields,
        HierarchyConfig hierarchy,
        Map<String, List<ColumnsSourceConfig>> columnsSources,
        Map<String, List<RelationConfig>> relations,
        Map<String, List<String>> enumTypes
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ClassConfig(
            String name,
            String sourceValue,
            boolean isAbstract,
            String rootType,
            String mainTable,
            String columnsTable,
            String dataTable
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldsConfig(
            List<MainFieldConfig> mainFields,
            List<ColumnsFieldConfig> columnsFields,
            List<DeclaredFieldConfig> declaredFields
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MainFieldConfig(
            String name,
            String db,
            String type,
            String enumTypeName,
            Boolean calculated
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ColumnsFieldConfig(
            String name,
            String db,
            String type,
            String enumTypeName
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DeclaredFieldConfig(
            String name,
            String type,
            String enumTypeName
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
    public record ColumnsSourceConfig(
            String source,
            String table,
            String columnsRef
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
