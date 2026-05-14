package com.enricher.service.registry;

import com.enricher.service.domain.MetadataExportV2;
import com.enricher.service.domain.ObjectClassInfo;
import com.enricher.service.util.NormalizeUtils;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class FieldRegistry {
    private static final Set<String> IMPLICIT_FIELDS = Set.of("id", "globalid", "objectclass");

    private final Map<ObjectClassInfo, Set<String>> declaredFieldsByClass;
    private final ObjectClassHierarchyRegistry hierarchyRegistry;

    public FieldRegistry(MetadataExportV2 metadata,
                         ObjectClassRegistry objectClassRegistry,
                         ObjectClassHierarchyRegistry hierarchyRegistry) {
        this.declaredFieldsByClass = new HashMap<>();
        this.hierarchyRegistry = hierarchyRegistry;

        if (metadata == null || metadata.fields() == null) {
            return;
        }

        for (Map.Entry<String, MetadataExportV2.FieldsConfig> entry : metadata.fields().entrySet()) {
            ObjectClassInfo objectClass = objectClassRegistry.byName(entry.getKey());
            if (objectClass == null || entry.getValue() == null) {
                continue;
            }

            Set<String> fields = new HashSet<>();
            addFields(fields, entry.getValue().declaredFields());
            fields.addAll(IMPLICIT_FIELDS);
            declaredFieldsByClass.put(objectClass, Set.copyOf(fields));
        }
    }

    public boolean hasField(ObjectClassInfo objectClass, String field) {
        if (objectClass == null || field == null || field.isBlank()) {
            return false;
        }
        Set<String> fields = declaredFieldsByClass.get(objectClass);
        if (fields == null || fields.isEmpty()) {
            return false;
        }
        return fields.contains(NormalizeUtils.lowerTrim(field));
    }

    public boolean hasFieldInHierarchy(ObjectClassInfo objectClass, String field) {
        if (objectClass == null || field == null || field.isBlank()) {
            return false;
        }
        for (ObjectClassInfo candidate : hierarchyRegistry.parentsOrSelfOrdered(objectClass)) {
            if (hasField(candidate, field)) {
                return true;
            }
        }
        return false;
    }

    private void addFields(Set<String> sink, List<MetadataExportV2.FieldConfig> fields) {
        if (fields == null || fields.isEmpty()) {
            return;
        }
        for (MetadataExportV2.FieldConfig field : fields) {
            if (field == null || field.name() == null || field.name().isBlank()) {
                continue;
            }
            sink.add(NormalizeUtils.lowerTrim(field.name()));
        }
    }
}
