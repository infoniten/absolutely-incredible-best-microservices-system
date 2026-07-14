package com.enricher.service.registry;

import com.enricher.service.domain.MetadataExportV3;
import com.enricher.service.domain.ObjectClassInfo;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class ObjectClassHierarchyRegistry {
    private final Map<ObjectClassInfo, Set<ObjectClassInfo>> parentsOrSelf;
    private final Map<ObjectClassInfo, List<ObjectClassInfo>> parentsOrSelfOrdered;
    private final Map<ObjectClassInfo, Set<ObjectClassInfo>> descendantsOrSelfByClass;

    public ObjectClassHierarchyRegistry(MetadataExportV3 metadata, ObjectClassRegistry objectClassRegistry) {
        this.parentsOrSelf = new HashMap<>();
        this.parentsOrSelfOrdered = new HashMap<>();
        this.descendantsOrSelfByClass = new HashMap<>();

        if (metadata == null || metadata.hierarchy() == null || metadata.hierarchy().parentsOrSelf() == null) {
            return;
        }

        Map<ObjectClassInfo, Set<ObjectClassInfo>> descendantsOrSelf = new HashMap<>();
        for (ObjectClassInfo objectClass : objectClassRegistry.all()) {
            descendantsOrSelf.put(objectClass, new HashSet<>(Set.of(objectClass)));
        }

        for (Map.Entry<String, List<String>> entry : metadata.hierarchy().parentsOrSelf().entrySet()) {
            ObjectClassInfo child = objectClassRegistry.byName(entry.getKey());
            if (child == null) {
                continue;
            }
            Set<ObjectClassInfo> parentSet = new HashSet<>();
            List<ObjectClassInfo> ordered = new ArrayList<>();
            if (entry.getValue() != null) {
                for (String className : entry.getValue()) {
                    ObjectClassInfo parent = objectClassRegistry.byName(className);
                    if (parent == null) {
                        continue;
                    }
                    parentSet.add(parent);
                    ordered.add(parent);
                }
            }
            parentsOrSelf.put(child, Set.copyOf(parentSet));
            parentsOrSelfOrdered.put(child, List.copyOf(ordered));

            for (ObjectClassInfo parent : parentSet) {
                if (parent != child) {
                    descendantsOrSelf.get(parent).add(child);
                }
            }
        }

        for (ObjectClassInfo objectClass : objectClassRegistry.all()) {
            descendantsOrSelfByClass.put(objectClass, Set.copyOf(descendantsOrSelf.get(objectClass)));
        }
    }

    public boolean isParentOrSelf(ObjectClassInfo parent, ObjectClassInfo child) {
        if (parent == null || child == null) {
            return false;
        }
        Set<ObjectClassInfo> set = parentsOrSelf.get(child);
        return set != null && set.contains(parent);
    }

    public List<ObjectClassInfo> parentsOrSelfOrdered(ObjectClassInfo objectClass) {
        if (objectClass == null) {
            return List.of();
        }
        List<ObjectClassInfo> chain = parentsOrSelfOrdered.get(objectClass);
        if (chain == null || chain.isEmpty()) {
            return List.of(objectClass);
        }
        return List.copyOf(chain);
    }

    public Set<ObjectClassInfo> descendantsOrSelf(ObjectClassInfo objectClass) {
        if (objectClass == null) {
            return Set.of();
        }
        Set<ObjectClassInfo> result = descendantsOrSelfByClass.get(objectClass);
        if (result == null) {
            throw new IllegalArgumentException("Unknown object class: [" + objectClass.name() + "]");
        }
        return result;
    }
}
