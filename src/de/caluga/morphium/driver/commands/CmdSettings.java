package de.caluga.morphium.driver.commands;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.Doc;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CmdSettings<T extends CmdSettings> {

    private static AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
    private String $db;
    private String coll;

    private String comment;

    public String getDb() {
        return $db;
    }

    public T setDb(String db) {
        this.$db = db;
        return (T) this;
    }

    public String getColl() {
        return coll;
    }

    public T setColl(String coll) {
        this.coll = coll;
        return (T) this;
    }

    public String getComment() {
        return comment;
    }

    public T setComment(String c) {
        comment = c;
        return (T) this;
    }


    public Doc asMap(String commandName) {
        Object o;
        Doc map = new Doc();
        map.put(commandName, getColl());
        for (Field f : an.getAllFields(this.getClass())) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            f.setAccessible(true);
            if (f.getName().equals("coll")) {
                continue;
            }

            try {
                Object v = f.get(this);
                if (v != null)
                    map.put(f.getName(), v);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return map;
    }

}
