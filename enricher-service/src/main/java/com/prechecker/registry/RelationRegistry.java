package com.prechecker.registry;

import com.prechecker.domain.MetadataExportV2;
import com.prechecker.domain.ObjectClassInfo;
import com.prechecker.util.NormalizeUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class RelationRegistry {
    private final Map<ObjectClassInfo, Map<String, RelationDef>> bySourceClassToken;
    private final Map<ObjectClassInfo, Map<String, RelationDef>> bySourceClassTokenWithParents;

    public RelationRegistry(MetadataExportV2 metadata,
                            ObjectClassRegistry objectClassRegistry,
                            ObjectClassHierarchyRegistry hierarchyRegistry) {
        this.bySourceClassToken = new HashMap<>();
        this.bySourceClassTokenWithParents = new HashMap<>();

        if (metadata != null && metadata.relations() != null) {
            for (Map.Entry<String, List<MetadataExportV2.RelationConfig>> entry : metadata.relations().entrySet()) {
                ObjectClassInfo sourceClass = objectClassRegistry.byName(entry.getKey());
                if (sourceClass == null) {
                    continue;
                }
                List<RelationDef> defs = new ArrayList<>();
                List<MetadataExportV2.RelationConfig> relationConfigs = entry.getValue();
                if (relationConfigs != null) {
                    for (MetadataExportV2.RelationConfig relationConfig : relationConfigs) {
                        RelationDef def = toRelationDef(objectClassRegistry, sourceClass, relationConfig);
                        if (def != null) {
                            defs.add(def);
                        }
                    }
                }
                bySourceClassToken.put(sourceClass, buildTokenMap(defs));
            }
        }

        for (ObjectClassInfo objectClass : objectClassRegistry.all()) {
            List<ObjectClassInfo> chain = hierarchyRegistry.parentsOrSelfOrdered(objectClass);
            Map<String, RelationDef> merged = new LinkedHashMap<>();
            for (ObjectClassInfo owner : chain) {
                Map<String, RelationDef> tokenMap = bySourceClassToken.get(owner);
                if (tokenMap == null || tokenMap.isEmpty()) {
                    continue;
                }
                for (Map.Entry<String, RelationDef> tokenEntry : tokenMap.entrySet()) {
                    merged.putIfAbsent(tokenEntry.getKey(), tokenEntry.getValue());
                }
            }
            bySourceClassTokenWithParents.put(objectClass, Map.copyOf(merged));
            bySourceClassToken.putIfAbsent(objectClass, Map.of());
        }
    }

    public RelationDef resolve(ObjectClassInfo objectClass, String token) {
        if (objectClass == null || token == null || token.isBlank()) {
            return null;
        }
        Map<String, RelationDef> defs = bySourceClassToken.get(objectClass);
        if (defs == null || defs.isEmpty()) {
            return null;
        }
        return defs.get(NormalizeUtils.lowerTrim(token));
    }

    public RelationDef resolveInHierarchy(ObjectClassInfo objectClass, String token) {
        if (objectClass == null || token == null || token.isBlank()) {
            return null;
        }
        Map<String, RelationDef> defs = bySourceClassTokenWithParents.get(objectClass);
        if (defs == null || defs.isEmpty()) {
            return null;
        }
        return defs.get(NormalizeUtils.lowerTrim(token));
    }

    private RelationDef toRelationDef(ObjectClassRegistry objectClassRegistry,
                                      ObjectClassInfo sourceClass,
                                      MetadataExportV2.RelationConfig relationConfig) {
        if (relationConfig == null || relationConfig.type() == null || relationConfig.type().isBlank()) {
            return null;
        }
        RelationType type = RelationType.fromValue(relationConfig.type());
        if (type == null) {
            return null;
        }
        ObjectClassInfo targetClass = objectClassRegistry.byName(relationConfig.targetClass());
        if (targetClass == null) {
            return null;
        }

        return new RelationDef(
                type,
                normalizeBlank(relationConfig.name()),
                normalizeBlank(relationConfig.alias()),
                normalizeBlank(relationConfig.sourceFieldName()),
                normalizeBlank(relationConfig.sourceJsonName()),
                sourceClass,
                targetClass
        );
    }

    private Map<String, RelationDef> buildTokenMap(List<RelationDef> defs) {
        if (defs == null || defs.isEmpty()) {
            return Map.of();
        }
        Map<String, RelationDef> result = new LinkedHashMap<>();
        for (RelationDef def : defs) {
            if (def == null) {
                continue;
            }
            String token = def.matchToken();
            if (token == null || token.isBlank()) {
                continue;
            }
            result.putIfAbsent(token, def);
        }
        return Map.copyOf(result);
    }

    private static String normalizeBlank(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public enum RelationType {
        GLOBAL_LINK,
        GLOBAL_SET,
        GLOBAL_ITEM,
        EMBEDDED_SET,
        EMBEDDED_ITEM;

        public static RelationType fromValue(String value) {
            if (value == null || value.isBlank()) {
                return null;
            }
            String normalized = value.trim().toUpperCase(Locale.ROOT);
            try {
                return RelationType.valueOf(normalized);
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }
    }

    public record RelationDef(RelationType type,
                              String name,
                              String alias,
                              String sourceFieldName,
                              String sourceJsonName,
                              ObjectClassInfo sourceClass,
                              ObjectClassInfo targetClass) {
        String matchToken() {
            return switch (type) {
                case GLOBAL_LINK -> NormalizeUtils.lowerTrim(alias);
                case GLOBAL_SET, GLOBAL_ITEM, EMBEDDED_SET, EMBEDDED_ITEM -> NormalizeUtils.lowerTrim(name);
            };
        }
    }
}
