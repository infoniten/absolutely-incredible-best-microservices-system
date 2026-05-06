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
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Supplier;

@Service
public class EnrichmentService {
    private record RequestMetricNames(String count, String errors, String duration) {
    }

    private final SearchServiceClient searchServiceClient;
    private final ObjectClassRegistry objectClassRegistry;
    private final ObjectClassHierarchyRegistry hierarchyRegistry;
    private final FieldRegistry fieldRegistry;
    private final RelationRegistry relationRegistry;
    private final JsonUtils jsonUtils;
    private final EnrichmentCacheService enrichmentCacheService;
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
                             EnrichmentCacheService enrichmentCacheService,
                             MeterRegistry meterRegistry) {
        this.searchServiceClient = searchServiceClient;
        this.objectClassRegistry = objectClassRegistry;
        this.hierarchyRegistry = hierarchyRegistry;
        this.fieldRegistry = fieldRegistry;
        this.relationRegistry = relationRegistry;
        this.jsonUtils = jsonUtils;
        this.enrichmentCacheService = enrichmentCacheService;
        this.meterRegistry = meterRegistry;
        this.enrichMetrics = metricNames("enricher.enrich");
        this.relationResolveCountMetric = "enricher.relation.resolve.count";
        this.projectionDepthCountMetric = "enricher.projection.depth.count";
    }

    public JsonNode enrich(String objectClassValue, long globalId, List<String> outputFields) {
        return recordRequest(
                enrichMetrics,
                () -> doEnrich(objectClassValue, globalId, outputFields),
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
        JsonNode rootObject = fetchByGlobalId(context, rootClass, globalId);

        if (outputFields == null || outputFields.isEmpty()) {
            return rootObject;
        }

        ParsedSelectors selectors = parseSelectors(rootClass, outputFields);
        validateSelectorRelations(selectors.sourceNodes());
        meterRegistry.counter(
                projectionDepthCountMetric,
                "object_class", normalizeTag(rootClass.sourceValue()),
                "depth", depthBucket(selectors.maxDepth())
        ).increment();

        String cacheKey = cacheKey(rootClass, globalId, outputFields);
        JsonNode cached = enrichmentCacheService.get(cacheKey);
        if (cached != null) {
            return cached;
        }

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

        enrichmentCacheService.put(cacheKey, result);
        return result;
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
            validateSelectorRelations(entry.getKey(), entry.getValue(), entry.getKey().sourceValue(), false);
        }
    }

    private void validateSelectorRelations(ObjectClassInfo currentClass,
                                           SelectorNode node,
                                           String pathPrefix,
                                           boolean allowParentRelationLookup) {
        if (currentClass == null || node == null || node.children.isEmpty()) {
            return;
        }

        for (Map.Entry<String, SelectorNode> childEntry : node.children.entrySet()) {
            String segment = childEntry.getKey();
            SelectorNode childNode = childEntry.getValue();
            String path = pathPrefix + "." + segment;

            RelationDef relation = allowParentRelationLookup
                    ? relationRegistry.resolveInHierarchy(currentClass, segment)
                    : relationRegistry.resolve(currentClass, segment);

            if (relation == null) {
                if (!childNode.children.isEmpty()) {
                    throw new IllegalArgumentException("Nested path is not supported for non-relation field: [" + path + "]");
                }
                boolean allowParentFieldLookup = allowParentRelationLookup;
                boolean fieldExists = allowParentFieldLookup
                        ? fieldRegistry.hasFieldInHierarchy(currentClass, segment)
                        : fieldRegistry.hasField(currentClass, segment);
                if (!fieldExists) {
                    throw new IllegalArgumentException("Unknown field in outputField path: [" + path + "]");
                }
                continue;
            }

            if (relation.type() != RelationType.GLOBAL_LINK && relation.type() != RelationType.EMBEDDED_SET) {
                throw new IllegalArgumentException("Relation type is not supported yet in enricher-service: [" + relation.type() + "]");
            }

            if (!childNode.children.isEmpty()) {
                validateSelectorRelations(relation.targetClass(), childNode, path, true);
            }
        }
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
                    throw new IllegalArgumentException("Nested path is not supported for non-relation field: [" + path + "]");
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
        meterRegistry.counter(
                relationResolveCountMetric,
                "relation_type", normalizeTag(relation.type().name()),
                "depth", depthBucket(depth)
        ).increment();

        if (relation.type() == RelationType.GLOBAL_LINK) {
            Long linkGlobalId = extractGlobalLinkId(currentObject, relation);
            if (linkGlobalId == null) {
                return NullNode.getInstance();
            }

            JsonNode relatedObject = fetchByGlobalId(context, relation.targetClass(), linkGlobalId);
            if (relationNode.children.isEmpty()) {
                return relatedObject;
            }
            return buildObjectProjection(
                    relation.targetClass(),
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
                return jsonUtils.createArrayNode();
            }

            ArrayNode collection = fetchByParentId(context, relation.targetClass(), parentId);
            if (relationNode.children.isEmpty()) {
                return collection;
            }

            ArrayNode projected = jsonUtils.createArrayNode();
            for (JsonNode item : collection) {
                projected.add(buildObjectProjection(
                        relation.targetClass(),
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
            return cached;
        }
        JsonNode value = searchServiceClient.getObjectByGlobalId(objectClass.sourceValue(), globalId);
        context.globalCache.put(key, value);
        return value;
    }

    private ArrayNode fetchByParentId(RuntimeContext context, ObjectClassInfo objectClass, long parentId) {
        String key = objectClass.sourceValueNormalized() + "|" + parentId;
        ArrayNode cached = context.parentCache.get(key);
        if (cached != null) {
            return cached;
        }
        JsonNode value = searchServiceClient.getObjectCollectionByParentId(objectClass.sourceValue(), parentId);
        if (value == null || !value.isArray()) {
            return jsonUtils.createArrayNode();
        }
        ArrayNode arrayNode = (ArrayNode) value;
        context.parentCache.put(key, arrayNode);
        return arrayNode;
    }

    private ObjectClassInfo resolveActualClass(JsonNode object, ObjectClassInfo fallbackRootClass) {
        String objectClassValue = null;
        if (object != null && object.has("objectClass") && object.get("objectClass").isTextual()) {
            objectClassValue = object.get("objectClass").asText();
        }
        ObjectClassInfo actualClass = objectClassRegistry.fromSourceValueOrName(objectClassValue);
        if (actualClass != null) {
            return actualClass;
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

    private Long extractGlobalLinkId(JsonNode object, RelationDef relation) {
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

        for (String candidate : candidates) {
            JsonNode value = readFieldValue(object, candidate);
            Long parsed = toLong(value);
            if (parsed != null) {
                return parsed;
            }
        }
        return null;
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

    private String cacheKey(ObjectClassInfo rootClass, long globalId, List<String> outputFields) {
        TreeSet<String> sortedFields = new TreeSet<>();
        for (String field : outputFields) {
            if (field == null || field.isBlank()) {
                continue;
            }
            sortedFields.add(field.trim());
        }
        return "enricher:v1:global:" + rootClass.sourceValue() + ":" + globalId + ":" + String.join("|", sortedFields);
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

    private static final class SelectorNode {
        private final Map<String, SelectorNode> children = new LinkedHashMap<>();
        private boolean terminal;
    }

    private static final class RuntimeContext {
        private final Map<String, JsonNode> globalCache = new HashMap<>();
        private final Map<String, ArrayNode> parentCache = new HashMap<>();
    }
}
