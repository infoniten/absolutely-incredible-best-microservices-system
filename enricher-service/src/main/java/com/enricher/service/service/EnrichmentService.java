package com.enricher.service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.enricher.service.domain.ObjectClassInfo;
import com.enricher.service.registry.FieldRegistry;
import com.enricher.service.registry.ObjectClassHierarchyRegistry;
import com.enricher.service.registry.ObjectClassRegistry;
import com.enricher.service.registry.RelationRegistry;
import com.enricher.service.registry.RelationRegistry.RelationDef;
import com.enricher.service.registry.RelationRegistry.RelationType;
import com.enricher.service.util.JsonUtils;
import com.enricher.service.util.NormalizeUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@Service
@Slf4j
public class EnrichmentService {
    private record RequestMetricNames(String count, String errors, String duration) {
    }

    private final SearchServiceClient searchServiceClient;
    private final ObjectClassRegistry objectClassRegistry;
    private final ObjectClassHierarchyRegistry hierarchyRegistry;
    private final FieldRegistry fieldRegistry;
    private final RelationRegistry relationRegistry;
    private final JsonUtils jsonUtils;
    private final MeterRegistry meterRegistry;
    private final RequestMetricNames enrichMetrics;
    private final String relationResolveCountMetric;
    private final String projectionDepthCountMetric;

    public EnrichmentService(SearchServiceClient searchServiceClient,
                             ObjectClassRegistry objectClassRegistry,
                             ObjectClassHierarchyRegistry hierarchyRegistry,
                             FieldRegistry fieldRegistry,
                             RelationRegistry relationRegistry,
                             JsonUtils jsonUtils,
                             MeterRegistry meterRegistry) {
        this.searchServiceClient = searchServiceClient;
        this.objectClassRegistry = objectClassRegistry;
        this.hierarchyRegistry = hierarchyRegistry;
        this.fieldRegistry = fieldRegistry;
        this.relationRegistry = relationRegistry;
        this.jsonUtils = jsonUtils;
        this.meterRegistry = meterRegistry;
        this.enrichMetrics = metricNames("enricher.enrich");
        this.relationResolveCountMetric = "enricher.relation.resolve.count";
        this.projectionDepthCountMetric = "enricher.projection.depth.count";
    }

    // TODO: Добавить настраиваемые ограничения количества outputField и глубины селектора,
    // чтобы ограничить рост числа вызовов downstream-сервисов.
    public JsonNode enrich(String objectClassValue, long globalId, List<String> outputFields) {
        log.info("Enrichment request: objectClass=[{}], globalId=[{}], outputFields=[{}]",
                objectClassValue, globalId, outputFields);
        return recordRequest(
                enrichMetrics,
                () -> doEnrich(objectClassValue, globalId, outputFields),
                "object_class", normalizeTag(objectClassValue),
                "mode", outputFields == null || outputFields.isEmpty() ? "full" : "projection",
                "output_fields_size", listSizeBucket(outputFields)
        );
    }

    // TODO: Добавить настраиваемые ограничения размера batch ревизий, количества outputField и глубины селектора.
    public JsonNode enrichRevision(String objectClassValue, List<Long> ids, List<String> outputFields) {
        log.info("Enrichment revision request: objectClass=[{}], ids=[{}], outputFields=[{}]",
                objectClassValue, ids, outputFields);
        if (ids == null || ids.isEmpty()){
            throw new IllegalArgumentException("Revision IDs list must not be empty");
        }

        for (Long id: ids){
            if (id == null){
                throw new IllegalArgumentException("revision id must not be null");
            }
        }
        return recordRequest(
                enrichMetrics,
                () -> doEnrichRevision(objectClassValue, ids, outputFields),
                "object_class", normalizeTag(objectClassValue),
                "mode", outputFields == null || outputFields.isEmpty() ? "full" : "projection",
                "output_fields_size", listSizeBucket(outputFields)
        );
    }

    private JsonNode doEnrich(String objectClassValue, long globalId, List<String> outputFields) {
        ObjectClassInfo rootClass = objectClassRegistry.fromSourceValueOrName(objectClassValue);
        if (rootClass == null) {
            throw new IllegalArgumentException("Invalid objectClass: [" + objectClassValue + "]");
        }

        RuntimeContext context = new RuntimeContext();
        ParsedSelectors selectors = prepareSelectors(rootClass, outputFields);
        JsonNode rootObject = fetchByGlobalId(context, rootClass, globalId);
        log.info("Root object loaded: objectClass=[{}], globalId=[{}]", rootClass.sourceValue(), globalId);
        return projectResponse(context, rootClass, rootObject, outputFields, selectors,
                "Enrichment full response prepared: objectClass=[{}], globalId=[{}]",
                "Enrichment projection prepared: rootClass=[{}], actualClass=[{}], globalId=[{}], outputFields=[{}], depth=[{}]",
                Long.toString(globalId));
    }

    private JsonNode doEnrichRevision(String objectClassValue, List<Long> ids, List<String> outputFields) {
        ObjectClassInfo rootClass = objectClassRegistry.fromSourceValueOrName(objectClassValue);
        if (rootClass == null) {
            throw new IllegalArgumentException("Invalid objectClass: [" + objectClassValue + "]");
        }

        RuntimeContext context = new RuntimeContext();
        ParsedSelectors selectors = prepareSelectors(rootClass, outputFields);
        ArrayNode rootObjects = fetchByIds(rootClass, ids);
        log.info("Root revision loaded: objectClass=[{}], id=[{}], count=[{}]", rootClass.sourceValue(), ids, rootObjects.size());

        ArrayNode result = jsonUtils.createArrayNode();

        for(JsonNode rootObject: rootObjects){
            Long id = extractObjectId(rootObject);
            result.add(projectResponse(context, rootClass, rootObject, outputFields, selectors,
                    "Enrichment revision full response prepared: objectClass=[{}], id=[{}]",
                    "Enrichment revision projection prepared: rootClass=[{}], actualClass=[{}], id=[{}], outputFields=[{}], depth=[{}]",
                    id == null ? "null" : Long.toString(id)));
        }

        return result;
    }

    private JsonNode projectResponse(RuntimeContext context,
                                     ObjectClassInfo rootClass,
                                     JsonNode rootObject,
                                     List<String> outputFields,
                                     ParsedSelectors selectors,
                                     String fullLogTemplate,
                                     String projectionLogTemplate,
                                     String lookupIdValue) {
        if (outputFields == null || outputFields.isEmpty()) {
            log.info(fullLogTemplate, rootClass.sourceValue(), lookupIdValue);
            return rootObject;
        }

        meterRegistry.counter(
                projectionDepthCountMetric,
                "object_class", normalizeTag(rootClass.sourceValue()),
                "depth", depthBucket(selectors.maxDepth())
        ).increment();

        ObjectClassInfo actualClass = resolveActualClass(rootObject, rootClass);
        ObjectNode result = jsonUtils.createObjectNode();
        result.put("objectClass", actualClass.sourceValue());

        for (Map.Entry<ObjectClassInfo, SelectorNode> sourceEntry : selectors.sourceNodes().entrySet()) {
            ObjectClassInfo sourceClass = sourceEntry.getKey();
            SelectorNode selectorNode = sourceEntry.getValue();
            JsonNode projected;
            if (!hierarchyRegistry.isParentOrSelf(sourceClass, actualClass)) {
                projected = buildNullObject(selectorNode);
            } else {
                projected = buildObjectProjection(
                        sourceClass,
                        rootObject,
                        selectorNode,
                        context,
                        0,
                        sourceClass.sourceValue()
                );
            }
            mergeProjection(result, projected);
        }
        log.info(projectionLogTemplate,
                rootClass.sourceValue(), actualClass.sourceValue(), lookupIdValue, outputFields, selectors.maxDepth());
        return result;
    }

    private ParsedSelectors prepareSelectors(ObjectClassInfo rootClass, List<String> outputFields) {
        if (outputFields == null || outputFields.isEmpty()) {
            return null;
        }

        List<SelectorCandidate> validSelectors = new ArrayList<>();
        for (String outputField : outputFields) {
            try {
                ParsedSelectors selector = parseSelectors(rootClass, Collections.singletonList(outputField));
                validateSelectorRelations(selector.sourceNodes());
                validSelectors.add(new SelectorCandidate(outputField, selector));
            } catch (IllegalArgumentException ex) {
                log.warn("Skipping invalid outputField: selector=[{}], reason=[{}]", outputField, ex.getMessage());
            }
        }

        validSelectors.sort((left, right) -> Integer.compare(
                right.selectors().maxDepth(),
                left.selectors().maxDepth()
        ));

        Map<ObjectClassInfo, SelectorNode> validSourceNodes = new LinkedHashMap<>();
        int maxDepth = 0;
        for (SelectorCandidate candidate : validSelectors) {
            try {
                mergeSelectors(validSourceNodes, candidate.selectors().sourceNodes());
                maxDepth = Math.max(maxDepth, candidate.selectors().maxDepth());
            } catch (IllegalArgumentException ex) {
                log.warn("Skipping conflicting outputField: selector=[{}], reason=[{}]",
                        candidate.value(), ex.getMessage());
            }
        }
        return new ParsedSelectors(
                Collections.unmodifiableMap(new LinkedHashMap<>(validSourceNodes)),
                maxDepth
        );
    }

    private void mergeSelectors(Map<ObjectClassInfo, SelectorNode> target,
                                Map<ObjectClassInfo, SelectorNode> source) {
        for (Map.Entry<ObjectClassInfo, SelectorNode> entry : source.entrySet()) {
            SelectorNode targetNode = target.computeIfAbsent(entry.getKey(), ignored -> new SelectorNode());
            ensureMergeCompatible(targetNode, entry.getValue(), entry.getKey().sourceValue());
            mergeSelectorNode(targetNode, entry.getValue());
        }
    }

    private void ensureMergeCompatible(SelectorNode target, SelectorNode source, String path) {
        if ((target.terminal && !source.children.isEmpty()) || (source.terminal && !target.children.isEmpty())) {
            throw new IllegalArgumentException("Cannot request both full relation and nested fields: [" + path + "]");
        }
        for (Map.Entry<String, SelectorNode> entry : source.children.entrySet()) {
            SelectorNode existingChild = target.children.get(entry.getKey());
            if (existingChild != null) {
                ensureMergeCompatible(existingChild, entry.getValue(), path + "." + entry.getKey());
            }
        }
    }

    private void mergeSelectorNode(SelectorNode target, SelectorNode source) {
        target.terminal = target.terminal || source.terminal;
        for (Map.Entry<String, SelectorNode> entry : source.children.entrySet()) {
            SelectorNode targetChild = target.children.computeIfAbsent(entry.getKey(), ignored -> new SelectorNode());
            mergeSelectorNode(targetChild, entry.getValue());
        }
    }

    private ParsedSelectors parseSelectors(ObjectClassInfo rootClass, List<String> outputFields) {
        Map<ObjectClassInfo, SelectorNode> sourceNodes = new LinkedHashMap<>();
        int maxDepth = 0;

        for (String selector : outputFields) {
            if (selector == null || selector.isBlank()) {
                throw new IllegalArgumentException("outputField contains empty selector");
            }
            String[] parts = selector.trim().split("\\.");
            if (parts.length < 2) {
                throw new IllegalArgumentException("Invalid outputField selector: [" + selector + "], expected source.path");
            }

            String sourceToken = parts[0].trim();
            ObjectClassInfo sourceClass = objectClassRegistry.fromSourceValueOrName(sourceToken);
            if (sourceClass == null) {
                throw new IllegalArgumentException("Unknown source in outputField: [" + sourceToken + "]");
            }

            if (!hierarchyRegistry.isParentOrSelf(rootClass, sourceClass)
                    && !hierarchyRegistry.isParentOrSelf(sourceClass, rootClass)) {
                throw new IllegalArgumentException("Source does not belong to hierarchy: [" + sourceToken + "], objectClass=["
                        + rootClass.sourceValue() + "]");
            }

            SelectorNode root = sourceNodes.computeIfAbsent(sourceClass, ignored -> new SelectorNode());
            SelectorNode current = root;
            int depth = 0;
            for (int i = 1; i < parts.length; i++) {
                String segment = parts[i].trim();
                validateSegment(segment, selector);
                current = current.children.computeIfAbsent(segment, ignored -> new SelectorNode());
                depth++;
            }
            current.terminal = true;
            maxDepth = Math.max(maxDepth, depth);
        }

        return new ParsedSelectors(Collections.unmodifiableMap(new LinkedHashMap<>(sourceNodes)), maxDepth);
    }

    private void validateSelectorRelations(Map<ObjectClassInfo, SelectorNode> sourceNodes) {
        for (Map.Entry<ObjectClassInfo, SelectorNode> entry : sourceNodes.entrySet()) {
            validateSelectorRelations(Set.of(entry.getKey()), entry.getValue(), entry.getKey().sourceValue(), false);
        }
    }

    private void validateSelectorRelations(Set<ObjectClassInfo> possibleClassRoots,
                                           SelectorNode node,
                                           String pathPrefix,
                                           boolean polymorphicLookup) {
        if (possibleClassRoots == null || possibleClassRoots.isEmpty() || node == null || node.children.isEmpty()) {
            return;
        }

        for (Map.Entry<String, SelectorNode> childEntry : node.children.entrySet()) {
            String segment = childEntry.getKey();
            SelectorNode childNode = childEntry.getValue();
            String path = pathPrefix + "." + segment;

            Map<ObjectClassInfo, RelationDef> relationsByActualClass = new HashMap<>();
            for (ObjectClassInfo possibleClassRoot : possibleClassRoots) {
                if (polymorphicLookup) {
                    relationsByActualClass.putAll(relationRegistry.resolvePolymorphic(possibleClassRoot, segment));
                } else {
                    RelationDef relation = relationRegistry.resolve(possibleClassRoot, segment);
                    if (relation != null) {
                        relationsByActualClass.put(possibleClassRoot, relation);
                    }
                }
            }
            Set<RelationDef> relations = new HashSet<>(relationsByActualClass.values());

            if (relations.isEmpty()) {
                if (!childNode.children.isEmpty()) {
                    throw new IllegalArgumentException("Nested path is not supported for non-relation field: [" + path + "]");
                }
                boolean fieldExists = possibleClassRoots.stream()
                        .anyMatch(possibleClassRoot -> polymorphicLookup
                                ? fieldRegistry.hasFieldInPolymorphicHierarchy(possibleClassRoot, segment)
                                : fieldRegistry.hasField(possibleClassRoot, segment));
                if (!fieldExists) {
                    throw new IllegalArgumentException("Unknown field in outputField path: [" + path + "]");
                }
                continue;
            }

            RelationDef unsupportedRelation = relations.stream()
                    .filter(relation -> !isSupportedRelation(relation))
                    .findFirst()
                    .orElse(null);
            if (unsupportedRelation != null) {
                throw new IllegalArgumentException("Relation type is not supported yet in enricher-service: ["
                        + unsupportedRelation.type() + "]");
            }

            if (!childNode.children.isEmpty()) {
                Set<ObjectClassInfo> targetClassRoots = new HashSet<>();
                for (RelationDef relation : relations) {
                    targetClassRoots.add(relation.targetClass());
                }
                validateSelectorRelations(targetClassRoots, childNode, path, true);
            }
        }
    }

    private boolean isSupportedRelation(RelationDef relation) {
        return relation.type() == RelationType.GLOBAL_LINK
                || relation.type() == RelationType.GLOBAL_ITEM
                || relation.type() == RelationType.EMBEDDED_SET;
    }

    private JsonNode buildObjectProjection(ObjectClassInfo currentClass,
                                           JsonNode currentObject,
                                           SelectorNode node,
                                           RuntimeContext context,
                                           int depth,
                                           String pathPrefix) {
        ObjectNode out = jsonUtils.createObjectNode();
        for (Map.Entry<String, SelectorNode> entry : node.children.entrySet()) {
            String segment = entry.getKey();
            SelectorNode childNode = entry.getValue();
            String path = pathPrefix + "." + segment;

            boolean allowParentLookup = depth > 0;
            RelationDef relation = allowParentLookup
                    ? relationRegistry.resolveInHierarchy(currentClass, segment)
                    : relationRegistry.resolve(currentClass, segment);

            JsonNode value;
            if (relation != null) {
                if (childNode.terminal && !childNode.children.isEmpty()) {
                    throw new IllegalArgumentException("Cannot request both full relation and nested fields: [" + path + "]");
                }
                value = buildRelationValue(relation, currentObject, childNode, context, depth + 1, path);
            } else {
                if (!childNode.children.isEmpty()) {
                    // Связь может быть объявлена только у другого наследника полиморфного target-класса.
                    // Статическая валидация уже проверила путь по всем возможным наследникам.
                    value = NullNode.getInstance();
                    out.set(segment, value);
                    continue;
                }
                value = readFieldValue(currentObject, segment);
            }
            out.set(segment, value);
        }
        return out;
    }

    private JsonNode buildRelationValue(RelationDef relation,
                                        JsonNode currentObject,
                                        SelectorNode relationNode,
                                        RuntimeContext context,
                                        int depth,
                                        String path) {
        log.debug("Resolving relation: path=[{}], relationType=[{}], targetClass=[{}], depth=[{}]",
                path, relation.type(), relation.targetClass().sourceValue(), depth);
        meterRegistry.counter(
                relationResolveCountMetric,
                "relation_type", normalizeTag(relation.type().name()),
                "depth", depthBucket(depth)
        ).increment();

        if (relation.type() == RelationType.GLOBAL_LINK) {
            Long linkGlobalId = extractRelationGlobalId(currentObject, relation);
            if (linkGlobalId == null) {
                log.debug("Relation GLOBAL_LINK is null: path=[{}]", path);
                return NullNode.getInstance();
            }

            JsonNode relatedObject = fetchByGlobalId(context, relation.targetClass(), linkGlobalId);
            if (relationNode.children.isEmpty()) {
                return relatedObject;
            }
            return buildObjectProjection(
                    resolveActualClass(relatedObject, relation.targetClass()),
                    relatedObject,
                    relationNode,
                    context,
                    depth,
                    path
            );
        }

        if (relation.type() == RelationType.GLOBAL_ITEM) {
            Long relationGlobalId = extractGlobalItemRelationGlobalId(currentObject, relation);
            if (relationGlobalId == null) {
                log.debug("Relation GLOBAL_ITEM id is null: path=[{}]", path);
                return NullNode.getInstance();
            }

            JsonNode relatedObject = fetchByGlobalItem(context, relation, relationGlobalId);
            if (relationNode.children.isEmpty()) {
                return relatedObject;
            }
            return buildObjectProjection(
                    resolveActualClass(relatedObject, relation.targetClass()),
                    relatedObject,
                    relationNode,
                    context,
                    depth,
                    path
            );
        }

        if (relation.type() == RelationType.EMBEDDED_SET) {
            Long parentId = extractObjectId(currentObject);
            if (parentId == null) {
                log.debug("Relation EMBEDDED_SET parent id is null: path=[{}]", path);
                return jsonUtils.createArrayNode();
            }

            ArrayNode collection = fetchByParentId(context, relation.targetClass(), parentId);
            if (relationNode.children.isEmpty()) {
                return collection;
            }

            ArrayNode projected = jsonUtils.createArrayNode();
            for (JsonNode item : collection) {
                projected.add(buildObjectProjection(
                        resolveActualClass(item, relation.targetClass()),
                        item,
                        relationNode,
                        context,
                        depth,
                        path
                ));
            }
            return projected;
        }

        throw new IllegalArgumentException("Unsupported relation type: [" + relation.type() + "]");
    }

    private JsonNode fetchByGlobalId(RuntimeContext context, ObjectClassInfo objectClass, long globalId) {
        String key = objectClass.sourceValueNormalized() + "|" + globalId;
        JsonNode cached = context.globalCache.get(key);
        if (cached != null) {
            log.debug("Global object found in enrichment context cache: objectClass=[{}], globalId=[{}]",
                    objectClass.sourceValue(), globalId);
            return cached;
        }
        JsonNode value = searchServiceClient.getObjectByGlobalId(objectClass.sourceValue(), globalId);
        context.globalCache.put(key, value);
        log.debug("Global object loaded from search-service: objectClass=[{}], globalId=[{}]",
                objectClass.sourceValue(), globalId);
        return value;
    }

    private ArrayNode fetchByParentId(RuntimeContext context, ObjectClassInfo objectClass, long parentId) {
        String key = objectClass.sourceValueNormalized() + "|" + parentId;
        ArrayNode cached = context.parentCache.get(key);
        if (cached != null) {
            log.debug("Parent collection found in enrichment context cache: objectClass=[{}], parentId=[{}]",
                    objectClass.sourceValue(), parentId);
            return cached;
        }
        JsonNode value = searchServiceClient.getObjectCollectionByParentId(objectClass.sourceValue(), parentId);
        if (value == null || !value.isArray()) {
            log.debug("Parent collection is empty: objectClass=[{}], parentId=[{}]",
                    objectClass.sourceValue(), parentId);
            return jsonUtils.createArrayNode();
        }
        ArrayNode arrayNode = (ArrayNode) value;
        context.parentCache.put(key, arrayNode);
        log.debug("Parent collection loaded from search-service: objectClass=[{}], parentId=[{}], count=[{}]",
                objectClass.sourceValue(), parentId, arrayNode.size());
        return arrayNode;
    }

    private ArrayNode fetchByIds(ObjectClassInfo objectClass, List<Long> ids) {
        JsonNode value = searchServiceClient.getObjectRevisionByIds(objectClass.sourceValue(), ids);
        if (value == null || !value.isArray()){
            log.debug("Revision collection is empty: objectClass=[{}], ids=[{}]", objectClass.sourceValue(), ids);
            return jsonUtils.createArrayNode();
        }

        ArrayNode arrayNode = (ArrayNode)value;

        log.debug("Revision loaded from search-service: objectClass=[{}], ids=[{}], count=[{}]",
                objectClass.sourceValue(), ids, arrayNode.size());
        return arrayNode;
    }

    private JsonNode fetchByGlobalItem(RuntimeContext context, RelationDef relation, long relationGlobalId) {
        requireGlobalItemMetadata(relation);
        String idFieldName = globalItemIdFieldName(relation);
        String roleFieldName = globalItemRoleFieldName(relation);
        String key = relation.targetClass().sourceValueNormalized()
                + "|" + idFieldName
                + "|" + roleFieldName
                + "|" + relation.roleValue()
                + "|" + relationGlobalId;
        JsonNode cached = context.globalItemCache.get(key);
        if (cached != null) {
            log.debug("Global-item object found in enrichment context cache: targetClass=[{}], relationGlobalId=[{}]",
                    relation.targetClass().sourceValue(), relationGlobalId);
            return cached;
        }
        JsonNode value = searchServiceClient.getObjectByGlobalItem(
                relation.targetClass().sourceValue(),
                relationGlobalId,
                idFieldName,
                roleFieldName,
                relation.roleValue()
        );
        context.globalItemCache.put(key, value);
        log.debug("Global-item object loaded from search-service: targetClass=[{}], relationGlobalId=[{}]",
                relation.targetClass().sourceValue(), relationGlobalId);
        return value;
    }

    private ObjectClassInfo resolveActualClass(JsonNode object, ObjectClassInfo fallbackRootClass) {
        if (object != null && object.has("objectClass") && object.get("objectClass").isTextual()) {
            ObjectClassInfo actualClass = objectClassRegistry.fromSourceValueOrName(object.get("objectClass").asText());
            if (actualClass != null) {
                return actualClass;
            }
        }

        // TODO: Удалить fallback на objectType после миграции legacy-ответов search-service на objectClass.
        if (object != null && object.has("objectType") && object.get("objectType").isTextual()) {
            ObjectClassInfo actualClass = objectClassRegistry.fromSourceValueOrName(object.get("objectType").asText());
            if (actualClass != null) {
                return actualClass;
            }
        }
        return fallbackRootClass;
    }

    private JsonNode buildNullObject(SelectorNode node) {
        ObjectNode out = jsonUtils.createObjectNode();
        for (String key : node.children.keySet()) {
            out.set(key, NullNode.getInstance());
        }
        return out;
    }

    private void mergeProjection(ObjectNode target, JsonNode projected) {
        if (target == null || projected == null || !projected.isObject()) {
            return;
        }
        projected.fields().forEachRemaining(entry -> target.set(entry.getKey(), entry.getValue()));
    }

    private Long extractRelationGlobalId(JsonNode object, RelationDef relation) {
        if (object == null || object.isNull()) {
            return null;
        }

        List<String> candidates = new ArrayList<>();
        if (relation.sourceJsonName() != null && !relation.sourceJsonName().isBlank()) {
            candidates.add(relation.sourceJsonName());
        }
        if (relation.sourceFieldName() != null && !relation.sourceFieldName().isBlank()) {
            candidates.add(relation.sourceFieldName());
            candidates.add(toCamelCase(relation.sourceFieldName()));
        }
        if (relation.name() != null && !relation.name().isBlank()) {
            candidates.add(relation.name());
        }
        if (relation.idFieldName() != null && !relation.idFieldName().isBlank()) {
            candidates.add(relation.idFieldName());
            candidates.add(toCamelCase(relation.idFieldName()));
        }
        if (relation.jsonIdFieldName() != null && !relation.jsonIdFieldName().isBlank()) {
            candidates.add(relation.jsonIdFieldName());
        }

        for (String candidate : candidates) {
            JsonNode value = readFieldValue(object, candidate);
            Long parsed = toLong(value);
            if (parsed != null) {
                return parsed;
            }
        }
        return null;
    }

    private Long extractGlobalItemRelationGlobalId(JsonNode object, RelationDef relation) {
        Long relationGlobalId = extractRelationGlobalId(object, relation);
        if (relationGlobalId != null) {
            return relationGlobalId;
        }
        return toLong(readFieldValue(object, "globalId"));
    }

    private void requireGlobalItemMetadata(RelationDef relation) {
        if (globalItemIdFieldName(relation) == null || globalItemIdFieldName(relation).isBlank()) {
            throw new IllegalArgumentException("GLOBAL_ITEM relation requires jsonIdFieldName: [" + relation.name() + "]");
        }
        if (globalItemRoleFieldName(relation) == null || globalItemRoleFieldName(relation).isBlank()) {
            throw new IllegalArgumentException("GLOBAL_ITEM relation requires jsonRoleFieldName: [" + relation.name() + "]");
        }
        if (relation.roleValue() == null || relation.roleValue().isBlank()) {
            throw new IllegalArgumentException("GLOBAL_ITEM relation requires roleValue: [" + relation.name() + "]");
        }
    }

    private String globalItemIdFieldName(RelationDef relation) {
        return relation.jsonIdFieldName();
    }

    private String globalItemRoleFieldName(RelationDef relation) {
        return relation.jsonRoleFieldName();
    }

    private Long extractObjectId(JsonNode object) {
        JsonNode idNode = readFieldValue(object, "id");
        return toLong(idNode);
    }

    private JsonNode readFieldValue(JsonNode object, String field) {
        if (object == null || object.isNull() || !object.isObject() || field == null || field.isBlank()) {
            return NullNode.getInstance();
        }

        if (object.has(field)) {
            JsonNode direct = object.get(field);
            return direct == null ? NullNode.getInstance() : direct;
        }

        String camel = toCamelCase(field);
        if (object.has(camel)) {
            JsonNode camelNode = object.get(camel);
            return camelNode == null ? NullNode.getInstance() : camelNode;
        }

        String snake = toSnakeCase(field);
        if (object.has(snake)) {
            JsonNode snakeNode = object.get(snake);
            return snakeNode == null ? NullNode.getInstance() : snakeNode;
        }

        String normalized = NormalizeUtils.lowerTrim(field);
        for (String candidate : iterable(object.fieldNames())) {
            if (NormalizeUtils.lowerTrim(candidate).equals(normalized)) {
                JsonNode candidateNode = object.get(candidate);
                return candidateNode == null ? NullNode.getInstance() : candidateNode;
            }
        }

        return NullNode.getInstance();
    }

    private List<String> iterable(java.util.Iterator<String> iterator) {
        List<String> items = new ArrayList<>();
        while (iterator.hasNext()) {
            items.add(iterator.next());
        }
        return items;
    }

    private Long toLong(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isIntegralNumber()) {
            return value.longValue();
        }
        if (value.isTextual()) {
            String text = value.asText().trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    private String toCamelCase(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        String[] parts = value.split("_");
        if (parts.length == 1) {
            return parts[0];
        }
        StringBuilder result = new StringBuilder(parts[0].toLowerCase());
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i].toLowerCase();
            if (part.isEmpty()) {
                continue;
            }
            result.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
        }
        return result.toString();
    }

    private String toSnakeCase(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) {
                out.append('_');
                out.append(Character.toLowerCase(ch));
            } else {
                out.append(Character.toLowerCase(ch));
            }
        }
        return out.toString();
    }

    private void validateSegment(String value, String selector) {
        if (value == null || value.isBlank() || !value.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("Invalid segment in outputField selector: [" + selector + "]");
        }
    }

    private <T> T recordRequest(RequestMetricNames metrics,
                                Supplier<T> action,
                                String... tags) {
        if ((tags.length & 1) == 1) {
            throw new IllegalArgumentException("Metric tags must be key-value pairs");
        }
        meterRegistry.counter(metrics.count(), tags).increment();
        try {
            return meterRegistry.timer(metrics.duration(), tags).record(action::get);
        } catch (RuntimeException ex) {
            meterRegistry.counter(metrics.errors(), tags).increment();
            throw ex;
        }
    }

    private static RequestMetricNames metricNames(String metricPrefix) {
        return new RequestMetricNames(
                metricPrefix + ".count",
                metricPrefix + ".errors",
                metricPrefix + ".duration"
        );
    }

    private String listSizeBucket(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "none";
        }
        int size = values.size();
        if (size == 1) {
            return "1";
        }
        if (size == 2) {
            return "2";
        }
        if (size <= 5) {
            return "3_5";
        }
        return "6_plus";
    }

    private String depthBucket(int depth) {
        if (depth <= 0) {
            return "0";
        }
        if (depth == 1) {
            return "1";
        }
        if (depth == 2) {
            return "2";
        }
        if (depth <= 5) {
            return "3_5";
        }
        return "6_plus";
    }

    private String normalizeTag(String value) {
        if (value == null || value.isBlank()) {
            return "none";
        }
        String source = value.trim().toLowerCase(Locale.ROOT);
        StringBuilder out = new StringBuilder(source.length());
        for (int i = 0; i < source.length(); i++) {
            char ch = source.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.') {
                out.append(ch);
            } else {
                out.append('_');
            }
        }
        return out.toString();
    }

    private record ParsedSelectors(Map<ObjectClassInfo, SelectorNode> sourceNodes, int maxDepth) {
    }

    private record SelectorCandidate(String value, ParsedSelectors selectors) {
    }

    private static final class SelectorNode {
        private final Map<String, SelectorNode> children = new LinkedHashMap<>();
        private boolean terminal;
    }

    private static final class RuntimeContext {
        private final Map<String, JsonNode> globalCache = new HashMap<>();
        private final Map<String, JsonNode> globalItemCache = new HashMap<>();
        private final Map<String, JsonNode> idCache = new HashMap<>();
        private final Map<String, ArrayNode> parentCache = new HashMap<>();
    }
}
