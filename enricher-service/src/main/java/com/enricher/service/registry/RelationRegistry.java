package com.enricher.service.registry;

import com.enricher.service.domain.MetadataExportV3;
import com.enricher.service.domain.ObjectClassInfo;
import com.enricher.service.util.NormalizeUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
@Slf4j
public class RelationRegistry {
    private final Map<ObjectClassInfo, Map<String, RelationDef>> bySourceClassToken;
    private final Map<ObjectClassInfo, Map<String, RelationDef>> bySourceClassTokenWithParents;
    private final Map<ObjectClassInfo, Map<String, Map<ObjectClassInfo, RelationDef>>> polymorphicRelations;

    public RelationRegistry(MetadataExportV3 metadata,
                            ObjectClassRegistry objectClassRegistry,
                            ObjectClassHierarchyRegistry hierarchyRegistry) {
        this.bySourceClassToken = new HashMap<>();
        this.bySourceClassTokenWithParents = new HashMap<>();
        this.polymorphicRelations = new HashMap<>();

        if (metadata != null && metadata.relations() != null) {
            for (Map.Entry<String, List<MetadataExportV3.RelationConfig>> entry : metadata.relations().entrySet()) {
                ObjectClassInfo sourceClass = objectClassRegistry.byName(entry.getKey());
                if (sourceClass == null) {
                    continue;
                }
                List<RelationDef> defs = new ArrayList<>();
                List<MetadataExportV3.RelationConfig> relationConfigs = entry.getValue();
                if (relationConfigs != null) {
                    for (MetadataExportV3.RelationConfig relationConfig : relationConfigs) {
                        RelationDef def = toRelationDef(objectClassRegistry, sourceClass, relationConfig);
                        if (def != null) {
                            defs.add(def);
                        }
                    }
                }
                bySourceClassToken.put(sourceClass, buildTokenMap(defs));
            }
        }

        warnAncestorDescendantTokenCollisions(objectClassRegistry, hierarchyRegistry);

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

        for (ObjectClassInfo baseClass : objectClassRegistry.all()) {
            Map<String, Map<ObjectClassInfo, RelationDef>> byTokenAndActualClass = new HashMap<>();
            for (ObjectClassInfo actualClass : hierarchyRegistry.descendantsOrSelf(baseClass)) {
                Map<String, RelationDef> effectiveRelations = bySourceClassTokenWithParents.get(actualClass);
                if (effectiveRelations == null || effectiveRelations.isEmpty()) {
                    continue;
                }
                for (Map.Entry<String, RelationDef> relationEntry : effectiveRelations.entrySet()) {
                    byTokenAndActualClass
                            .computeIfAbsent(relationEntry.getKey(), ignored -> new HashMap<>())
                            .put(actualClass, relationEntry.getValue());
                }
            }
            Map<String, Map<ObjectClassInfo, RelationDef>> immutableByToken = new HashMap<>();
            byTokenAndActualClass.forEach((token, byActualClass) ->
                    immutableByToken.put(token, Map.copyOf(byActualClass)));
            polymorphicRelations.put(baseClass, Map.copyOf(immutableByToken));
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

    public Map<ObjectClassInfo, RelationDef> resolvePolymorphic(ObjectClassInfo baseClass, String token) {
        if (baseClass == null || token == null || token.isBlank()) {
            return Map.of();
        }
        Map<String, Map<ObjectClassInfo, RelationDef>> byToken = polymorphicRelations.get(baseClass);
        if (byToken == null || byToken.isEmpty()) {
            return Map.of();
        }
        return byToken.getOrDefault(NormalizeUtils.lowerTrim(token), Map.of());
    }

    private void warnAncestorDescendantTokenCollisions(ObjectClassRegistry objectClassRegistry,
                                                       ObjectClassHierarchyRegistry hierarchyRegistry) {
        for (ObjectClassInfo descendant : objectClassRegistry.all()) {
            Map<String, RelationDef> descendantRelations = bySourceClassToken.get(descendant);
            if (descendantRelations == null || descendantRelations.isEmpty()) {
                continue;
            }
            for (ObjectClassInfo ancestor : hierarchyRegistry.parentsOrSelfOrdered(descendant)) {
                if (ancestor == descendant) {
                    continue;
                }
                Map<String, RelationDef> ancestorRelations = bySourceClassToken.get(ancestor);
                if (ancestorRelations == null || ancestorRelations.isEmpty()) {
                    continue;
                }
                for (Map.Entry<String, RelationDef> descendantEntry : descendantRelations.entrySet()) {
                    RelationDef ancestorRelation = ancestorRelations.get(descendantEntry.getKey());
                    if (ancestorRelation == null) {
                        continue;
                    }
                    log.warn("Relation token collision in class hierarchy: token=[{}], descendantClass=[{}], "
                                    + "ancestorClass=[{}], descendantTarget=[{}], ancestorTarget=[{}]; "
                                    + "descendant relation takes precedence",
                            descendantEntry.getKey(),
                            descendant.sourceValue(),
                            ancestor.sourceValue(),
                            descendantEntry.getValue().targetClass().sourceValue(),
                            ancestorRelation.targetClass().sourceValue());
                }
            }
        }
    }

    private RelationDef toRelationDef(ObjectClassRegistry objectClassRegistry,
                                      ObjectClassInfo sourceClass,
                                      MetadataExportV3.RelationConfig relationConfig) {
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
                normalizeBlank(relationConfig.idFieldName()),
                normalizeBlank(relationConfig.jsonIdFieldName()),
                normalizeBlank(relationConfig.roleFieldName()),
                normalizeBlank(relationConfig.jsonRoleFieldName()),
                normalizeBlank(relationConfig.roleValue()),
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
                              String idFieldName,
                              String jsonIdFieldName,
                              String roleFieldName,
                              String jsonRoleFieldName,
                              String roleValue,
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
