package com.prechecker.util;

import java.util.Locale;

public final class NormalizeUtils {
    private NormalizeUtils() {
    }

    public static String lowerTrim(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    public static String upperTrim(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
