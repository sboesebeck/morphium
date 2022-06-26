package de.caluga.morphium.driver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Doc extends LinkedHashMap<String, Object> {
    public Doc(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public Doc(int initialCapacity) {
        super(initialCapacity);
    }

    public Doc() {
    }

    public Doc(Map<? extends String, ?> m) {
        super();
        if (m != null) {
            putAll(m);
        }
    }

    public Doc(int initialCapacity, float loadFactor, boolean accessOrder) {
        super(initialCapacity, loadFactor, accessOrder);
    }

    public static Doc of() {
        return new Doc();
    }

    public static Doc of(String k1, Object v1) {
        return of().add(k1, v1);
    }

    public static Doc of(String k1, Object v1, String k2, Object v2) {
        return of().add(k1, v1)
                .add(k2, v2);
    }

    public static Doc of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return of().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3);
    }

    public static Doc of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        return of().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4);
    }

    public static Doc of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, String k5, Object v5) {
        return of().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4)
                .add(k5, v5);
    }

    public static Doc of(Map<String, Object> map) {
        if (map == null) return new Doc();
        return new Doc(map);
    }

    public static List<Doc> convertToDocList(List<Map<String, Object>> lst) {
        var ret = new ArrayList<Doc>();
        for (Map<String, Object> m : lst) {
            ret.add(Doc.of(m));
        }
        return ret;
    }

    public static List<Map<String, Object>> convertToMapList(List<Doc> lst) {
        var ret = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> m : lst) {
            Map<String, Object> map = new LinkedHashMap<>(m);
            ret.add(map);
        }
        return ret;
    }

    public Doc add(String k, Object value) {
        put(k, value);
        return this;
    }

    public Doc addNull(String k) {
        put(k, null);
        return this;
    }

    public Doc addIfNotNull(String k, Object value) {
        if (value != null) put(k, value);
        return this;
    }

}
