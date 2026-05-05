package com.prechecker.domain;

import java.util.List;
import java.util.Map;

public record MetadataExportV2(
        List<ClassConfig> classes,
        Map<String, FieldsConfig> fields,
        HierarchyConfig hierarchy,
        Map<String, List<IndexSourceConfig>> indexSources,
        Map<String, List<RelationConfig>> relations,
        Map<String, List<String>> enumTypes
) {
    public record ClassConfig(
            String name,
            String sourceValue,
            boolean isAbstract,
            String rootType,
            String mainTable,
            String indexTable,
            String dataTable
    ) {
    }

    public record FieldsConfig(
            List<FieldConfig> mainFields,
            List<FieldConfig> indexFields,
            List<FieldConfig> declaredFields
    ) {
    }

    public record FieldConfig(
            String name,
            String db,
            String type,
            String enumTypeName
    ) {
    }

    public record HierarchyConfig(
            Map<String, List<String>> parentsOrSelf,
            Map<String, String> rootUnderBase,
            Map<String, List<String>> actualClassesForRoot
    ) {
    }

    public record IndexSourceConfig(String source, String table, String columnsRef) {
    }

    public record RelationConfig(
            String name,
            String alias,
            String type,
            String targetClass,
            String sourceFieldName,
            String sourceJsonName,
            String idFieldName,
            String roleFieldName,
            String roleValue,
            String sourceFieldDeclaredInClass,
            String idFieldDeclaredInClass,
            String roleFieldDeclaredInClass
    ) {
    }
}
