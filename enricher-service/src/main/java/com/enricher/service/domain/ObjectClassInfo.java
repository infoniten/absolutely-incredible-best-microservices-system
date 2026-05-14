package com.enricher.service.domain;

import com.enricher.service.util.NormalizeUtils;

public final class ObjectClassInfo {
    private final String name;
    private final String sourceValue;
    private final String sourceValueNormalized;
    private final boolean isAbstract;

    public ObjectClassInfo(String name, String sourceValue, boolean isAbstract) {
        this.name = name;
        this.sourceValue = sourceValue;
        this.sourceValueNormalized = NormalizeUtils.lowerTrim(sourceValue);
        this.isAbstract = isAbstract;
    }

    public String name() {
        return name;
    }

    public String sourceValue() {
        return sourceValue;
    }

    public String sourceValueNormalized() {
        return sourceValueNormalized;
    }

    public boolean isAbstract() {
        return isAbstract;
    }
}
