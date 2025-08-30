package de.caluga.morphium.query;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

public final class FieldNames {
    private FieldNames() {}

    public static <T> String of(Property<T, ?> getter) {
        try {
            Method m = getter.getClass().getDeclaredMethod("writeReplace");
            m.setAccessible(true);
            Object replacement = m.invoke(getter);
            if (!(replacement instanceof SerializedLambda)) {
                throw new IllegalStateException("Not a SerializedLambda: " + replacement);
            }
            SerializedLambda sl = (SerializedLambda) replacement;
            String impl = sl.getImplMethodName();
            if (impl.startsWith("get") && impl.length() > 3) return decapitalize(impl.substring(3));
            if (impl.startsWith("is") && impl.length() > 2) return decapitalize(impl.substring(2));
            return impl;
        } catch (Exception e) {
            throw new RuntimeException("Cannot extract property name from lambda", e);
        }
    }

    private static String decapitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        if (s.length() == 1) return s.toLowerCase();
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }
}

