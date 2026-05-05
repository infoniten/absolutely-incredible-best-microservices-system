package com.prechecker.registry;

import com.prechecker.domain.MetadataExportV2;
import com.prechecker.domain.ObjectClassInfo;
import com.prechecker.util.NormalizeUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class ObjectClassRegistry {
    private final Map<String, ObjectClassInfo> byName;
    private final Map<String, ObjectClassInfo> bySource;

    public ObjectClassRegistry(MetadataExportV2 metadata) {
        this.byName = new HashMap<>();
        this.bySource = new HashMap<>();

        if (metadata == null || metadata.classes() == null) {
            return;
        }

        for (MetadataExportV2.ClassConfig classConfig : metadata.classes()) {
            if (classConfig == null) {
                continue;
            }
            String name = normalizeName(classConfig.name());
            if (name.isBlank()) {
                continue;
            }
            ObjectClassInfo info = new ObjectClassInfo(name, classConfig.sourceValue(), classConfig.isAbstract());
            byName.put(name, info);
            if (info.sourceValue() != null && !info.sourceValue().isBlank()) {
                bySource.put(info.sourceValueNormalized(), info);
            }
        }
    }

    public ObjectClassInfo byName(String name) {
        if (name == null) {
            return null;
        }
        return byName.get(normalizeName(name));
    }

    public ObjectClassInfo fromSourceValue(String value) {
        if (value == null) {
            return null;
        }
        return bySource.get(NormalizeUtils.lowerTrim(value));
    }

    public ObjectClassInfo fromSourceValueOrName(String value) {
        ObjectClassInfo bySourceValue = fromSourceValue(value);
        if (bySourceValue != null) {
            return bySourceValue;
        }
        return byName(value);
    }

    public List<ObjectClassInfo> all() {
        return new ArrayList<>(byName.values());
    }

    private static String normalizeName(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
