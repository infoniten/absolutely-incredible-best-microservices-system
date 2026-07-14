package com.enricher.service.registry;

import com.enricher.service.domain.MetadataExportV3;
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
    private final Map<ObjectClassInfo, Set<String>> fieldsInHierarchyByClass;
    private final Map<ObjectClassInfo, Set<String>> polymorphicFieldsByClass;

    public FieldRegistry(MetadataExportV3 metadata,
                         ObjectClassRegistry objectClassRegistry,
                         ObjectClassHierarchyRegistry hierarchyRegistry) {
        this.declaredFieldsByClass = new HashMap<>();
        this.fieldsInHierarchyByClass = new HashMap<>();
        this.polymorphicFieldsByClass = new HashMap<>();

        if (metadata != null && metadata.fields() != null) {
            for (Map.Entry<String, MetadataExportV3.FieldsConfig> entry : metadata.fields().entrySet()) {
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

        for (ObjectClassInfo objectClass : objectClassRegistry.all()) {
            Set<String> fields = new HashSet<>();
            for (ObjectClassInfo parentOrSelf : hierarchyRegistry.parentsOrSelfOrdered(objectClass)) {
                fields.addAll(declaredFieldsByClass.getOrDefault(parentOrSelf, Set.of()));
            }
            fieldsInHierarchyByClass.put(objectClass, Set.copyOf(fields));
        }

        for (ObjectClassInfo objectClass : objectClassRegistry.all()) {
            Set<String> fields = new HashSet<>();
            for (ObjectClassInfo descendantOrSelf : hierarchyRegistry.descendantsOrSelf(objectClass)) {
                fields.addAll(fieldsInHierarchyByClass.getOrDefault(descendantOrSelf, Set.of()));
            }
            polymorphicFieldsByClass.put(objectClass, Set.copyOf(fields));
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
        Set<String> fields = fieldsInHierarchyByClass.get(objectClass);
        return fields != null && fields.contains(NormalizeUtils.lowerTrim(field));
    }

    public boolean hasFieldInPolymorphicHierarchy(ObjectClassInfo objectClass, String field) {
        if (objectClass == null || field == null || field.isBlank()) {
            return false;
        }
        Set<String> fields = polymorphicFieldsByClass.get(objectClass);
        return fields != null && fields.contains(NormalizeUtils.lowerTrim(field));
    }

    private void addFields(Set<String> sink, List<MetadataExportV3.DeclaredFieldConfig> fields) {
        if (fields == null || fields.isEmpty()) {
            return;
        }
        for (MetadataExportV3.DeclaredFieldConfig field : fields) {
            if (field == null || field.name() == null || field.name().isBlank()) {
                continue;
            }
            sink.add(NormalizeUtils.lowerTrim(field.name()));
        }
    }
}
