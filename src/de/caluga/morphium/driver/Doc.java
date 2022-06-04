package de.caluga.morphium.driver;

import java.util.LinkedHashMap;
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
        super(m);
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
        return new Doc(map);
    }

    public Doc add(String k, Object value) {
        put(k, value);
        return this;
    }

}
