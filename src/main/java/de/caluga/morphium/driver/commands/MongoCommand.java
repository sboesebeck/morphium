package de.caluga.morphium.driver.commands;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.LoggerFactory;

public abstract class MongoCommand<T extends MongoCommand> {
    @Transient
    private static AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
    private String $db;
    private String coll;

    private String comment;

    private Map<String, Object> metaData;

    @Transient
    private MongoConnection connection;
    private Doc $readPreference = Doc.of("mode", "primaryPreferred");

    public MongoCommand(MongoConnection c) {
        connection = c;
    }

    public void releaseConnection() {
        RuntimeException ex=new RuntimeException();
        LoggerFactory.getLogger(MongoCommand.class).info("Release from: "+ex.getStackTrace()[1]);
        getConnection().release();
        connection=null;
    }


    public MongoConnection getConnection() {
        return connection;
    }

    public MongoCommand<T> setConnection(MongoConnection connection) {
        this.connection = connection;
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

    public T fromMap(Map<String,Object> m){
        setColl(""+m.get(getCommandName()));
        for (Field f : an.getAllFields(this.getClass())) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            if (f.isAnnotationPresent(Transient.class)) continue;
            if (f.getName().equals("metaData")) continue;
            if (f.getName().equals("readPreference")) continue;
            if (f.getName().equals("connection")) continue;
            if (f.getName().equals("coll")) continue;

            if (DriverTailableIterationCallback.class.isAssignableFrom(f.getType())) continue;
            if (AsyncOperationCallback.class.isAssignableFrom(f.getType())) continue;
            String n = f.getName();
            //TODO: find better solution
            if (n.equals("newFlag")) n = "new";
            f.setAccessible(true);

            try {
                Object v=m.get(n);
                if (v==null) continue;
                Class targetType = f.getType();
                if (targetType.isEnum()) {
                    Enum en=Enum.valueOf((Class<Enum>) f.getType(),v.toString());
                    v = en;
                }
                if (v != null) {
                    if (f.getType().equals(UUID.class)) {
                        f.set(this,((Map)v).get("id"));
                    } else {
                        f.set(this,v);
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
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
            if (f.isAnnotationPresent(Transient.class)) continue;
            if (f.getName().equals("metaData")) continue;
            if (f.getName().equals("readPreference")) continue;
            if (f.getName().equals("connection")) continue;
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
            if (f.getType().equals(MongoConnection.class)) {
                continue;
            }
            f.setAccessible(true);
            try {
                if (f.getType().isPrimitive()){
                   if (f.getType().equals(boolean.class)){
                        f.set(this,false);
                    } else if (f.getType().equals(int.class)){
                        f.set(this,0);
                    } else if (f.getType().equals(long.class)){
                        f.set(this,0L);
                    } else if (f.getType().equals(float.class)){
                        f.set(this,0.0f);
                    } else if (f.getType().equals(double.class)){
                        f.set(this,0.0d);
                    }
                } else {
                   f.set(this, null);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        $readPreference = Doc.of("mode", "primaryPreferred");
    }

    public abstract String getCommandName();

    public int executeAsync() throws MorphiumDriverException {
        MongoConnection con = getConnection();
        if (con == null) throw new IllegalArgumentException("you need to set the driver!");
        return new NetworkCallHelper<Integer>().doCall(()->{
            //long start = System.currentTimeMillis();
            var result = con.sendCommand(this);
            // long dur = System.currentTimeMillis() - start;
            setMetaData("duration", 0); //not waiting!
            setMetaData("server", con.getConnectedTo() + ":" + con.getConnectedToPort());
            return result;
        }, con.getDriver().getRetriesOnNetworkError(), con.getDriver().getSleepBetweenErrorRetries());
    }

}
