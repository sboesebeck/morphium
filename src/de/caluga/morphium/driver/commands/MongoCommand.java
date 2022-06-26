package de.caluga.morphium.driver.commands;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.sync.DriverBase;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MongoCommand<T extends MongoCommand> {
    private static AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
    private String $db;
    private String coll;

    private String comment;

    private Map<String, Object> metaData;
    private MorphiumDriver driver;

    public MongoCommand(MorphiumDriver d) {
        driver = d;
    }

    public MorphiumDriver getDriver() {
        return driver;
    }

    public MongoCommand<T> setDriver(DriverBase driver) {
        this.driver = driver;
        return this;
    }

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

    public MongoCommand<T> setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
        return this;
    }

    public MongoCommand<T> setMetaData(String key, Object value) {
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


    public Map<String, Object> asMap() {
        Object o;
        Doc map = new Doc();
        map.put(getCommandName(), getColl());
        for (Field f : an.getAllFields(this.getClass())) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            if (f.getName().equals("metaData")) continue;
            if (f.getName().equals("readPreference")) continue;
            if (f.getName().equals("driver")) continue;
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

    public void clear() {
        for (Field f : an.getAllFields(this.getClass())) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            f.setAccessible(true);
            try {
                f.set(this, null);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public abstract String getCommandName();


    public int executeAsync() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<Integer>().doCall(() -> {
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            //long start = System.currentTimeMillis();
            int id = driver.sendCommand(getDb(), asMap());
            // long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", 0); //not waiting!
            return id;
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }

    public boolean hasReplyFor(int cmdId) {
        return getDriver().replyAvailableFor(cmdId);
    }

    public Map<String, Object> getSingleResultFor(int cmdId) throws MorphiumDriverException {
        return getDriver().readSingleAnswer(cmdId);
    }

    public MorphiumCursor getAnswerFor(int cmdId) throws MorphiumDriverException {
        return getDriver().getAnswerFor(cmdId);
    }


}
