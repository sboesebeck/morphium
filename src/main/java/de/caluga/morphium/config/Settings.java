package de.caluga.morphium.config;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;

@Embedded
public abstract class Settings {

    public Properties asProperties() {
        return asProperties(null);
    }

    public Properties asProperties(String prefix) {
        Properties p = new Properties();
        if (prefix == null || prefix.isEmpty()) prefix = ""; else prefix = prefix + ".";
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);

        List<Field> flds = an.getAllFields(this.getClass());

        for (Field f : flds) {
            if (f.isAnnotationPresent(Transient.class)) {
                continue;
            }

            try {
                var defaults = this.getClass().getConstructor().newInstance();
                f.setAccessible(true);

                if (f.get(this) != null && !f.get(this).equals(f.get(defaults)) || f.getName().equals("database")) {
                    p.put(prefix + f.getName(), f.get(this).toString());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


        return p;
    }

}
