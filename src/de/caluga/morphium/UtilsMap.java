package de.caluga.morphium;

import java.util.LinkedHashMap;
import java.util.Map;

public class UtilsMap<K, V> extends LinkedHashMap<K, V> {
    public static <K, V> UtilsMap<K, V> of() {
        return new UtilsMap<>();
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1) {
        return new UtilsMap<K, V>().add(k1, v1);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2) {
        return new UtilsMap<K, V>().add(k1, v1).add(k2, v2);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        return new UtilsMap<K, V>().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        return new UtilsMap<K, V>().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        return new UtilsMap<K, V>().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4)
                .add(k5, v5);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        return new UtilsMap<K, V>().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4)
                .add(k5, v5)
                .add(k6, v6);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        return new UtilsMap<K, V>().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4)
                .add(k5, v5)
                .add(k6, v6)
                .add(k7, v7);
    }

    public static <K, V> UtilsMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8) {
        return new UtilsMap<K, V>().add(k1, v1)
                .add(k2, v2)
                .add(k3, v3)
                .add(k4, v4)
                .add(k5, v5)
                .add(k6, v6)
                .add(k7, v7)
                .add(k8, v8);
    }

    public UtilsMap<K, V> add(K key, V val) {
        if (val == null) return this;
        put(key, val);
        return this;
    }

    public UtilsMap<K, V> addAll(Map<K, V> m) {
        putAll(m);
        return this;
    }
}
