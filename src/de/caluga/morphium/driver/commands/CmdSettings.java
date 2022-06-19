package de.caluga.morphium.driver.commands;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.ReadPreference;

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
    private ReadPreference readPreference;
    private Map<String, Object> metaData;

    public String getDb() {
        return $db;
    }

    /**
     * will be set by the driver, containing information about
     * total runtime (duration)
     * host used (server)
     * and other meta information about the execution of this command
     *
     * @return
     */
    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public CmdSettings<T> setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
        return this;
    }

    public CmdSettings<T> setMetaData(String key, Object value) {
        if (metaData == null) metaData = new HashMap<>();
        metaData.put(key, value);
        return this;
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

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public T setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
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
            if (f.getName().equals("metaData")) continue;
            if (f.getName().equals("readPreference")) continue;
            if (f.getName().equals("coll")) continue;
            if (DriverTailableIterationCallback.class.isAssignableFrom(f.getType())) continue;
            if (AsyncOperationCallback.class.isAssignableFrom(f.getType())) continue;

            f.setAccessible(true);

            try {
                Object v = f.get(this);
                if (v instanceof Enum) {
                    v = v.toString();
                }
                String n = f.getName();
                //TODO: find better solution
                if (n.equals("newFlag")) n = "new";
                if (v != null)
                    map.put(n, v);

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return map;
    }

}
