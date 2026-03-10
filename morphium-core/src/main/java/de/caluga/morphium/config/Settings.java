package de.caluga.morphium.config;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import java.util.Objects;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;

@Embedded
public abstract class Settings {

    public Properties asProperties() {
        return asProperties(null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null) return false;
        if (getClass() != other.getClass()) return false;

        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        List<Field> flds = an.getAllFields(this.getClass());

        for (Field f : flds) {
            if (f.isAnnotationPresent(Transient.class)) {
                continue;
            }

            f.setAccessible(true);
            try {
                Object thisValue = f.get(this);
                Object otherValue = f.get(other);

                if (thisValue == otherValue) return true;
                if (thisValue == null || otherValue == null) return false;
                if (!thisValue.equals(otherValue)) return false;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        List<Field> flds = an.getAllFields(this.getClass());
        int result = 1;

        for (Field f : flds) {
            if (f.isAnnotationPresent(Transient.class)) continue;
            f.setAccessible(true);

            try {
                Object v = f.get(this);
                result = 31 * result + Objects.hashCode(v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return result;
    }

    public Properties asProperties(String prefix) {
        Properties p = new Properties();
        try {
            if (prefix == null || prefix.isEmpty()) prefix = ""; else prefix = prefix + ".";
            AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);

            List<Field> flds = an.getAllFields(this.getClass());

            var defaults = this.getClass().getConstructor().newInstance();
            for (Field f : flds) {
                if (f.isAnnotationPresent(Transient.class)) {
                    continue;
                }

                f.setAccessible(true);

                if (f.get(this) != null && !f.get(this).equals(f.get(defaults)) || f.getName().equals("database")) {
                    p.put(prefix + f.getName(), f.get(this).toString());
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        return p;
    }

    @SuppressWarnings("unchecked")
    public <T extends Settings> T copy() {
        try {
            T ret = (T) this.getClass().getConstructor().newInstance();
            AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
            List<Field> flds = an.getAllFields(this.getClass());

            for (Field f : flds) {
                if (f.isAnnotationPresent(Transient.class)) {
                    continue;
                }

                f.setAccessible(true);
                Object v = f.get(this);

                if (v == null) {
                    f.set(ret, null);
                } else if (v instanceof java.util.List) {
                    // shallow copy list container
                    f.set(ret, new java.util.ArrayList<>((java.util.List<?>) v));
                } else if (v instanceof java.util.Map) {
                    // shallow copy map container with ordering
                    f.set(ret, new java.util.LinkedHashMap<>((java.util.Map <?, ? >) v));
                } else if (v.getClass().isArray() && v instanceof Object[]) {
                    f.set(ret, ((Object[]) v).clone());
                } else {
                    // primitives, boxed types, enums, strings are safe to assign
                    f.set(ret, v);
                }
            }

            return ret;
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy settings for " + this.getClass().getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Settings> T copyWith(java.util.function.Consumer<T> mutator) {
        T c = (T) copy();
        if (mutator != null) mutator.accept(c);
        return c;
    }

}
