package de.caluga.morphium;

import de.caluga.morphium.MorphiumStorageListener.UpdateTypes;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.commands.ListDatabasesCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.writer.MorphiumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class MorphiumBase {
    private Logger log = LoggerFactory.getLogger(Morphium.class);

    // /**
    //  * This method unsets a property.
    //  *
    //  * @deprecated There is a newer implementation.
    //  * Please use {@link Morphium#unsetInEntity(Object, Enum)}
    //  */
    // @Deprecated
    // public <T> void unset(T toSet, Enum<?> field) {
    //     unsetInEntity(toSet, field.name());
    // }
    public <T> void unsetInEntity(T toSet, Enum<?> field) {
        unsetInEntity(toSet, field.name(), (AsyncOperationCallback) null);
    }

    // /**
    //  * This method unsets a property.
    //  *
    //  * @deprecated There is a newer implementation.
    //  * Please use {@link Morphium#unsetInEntity(Object, String)} ()} ()} instead.
    //  */
    // @Deprecated
    // public <T> void unset(final T toSet, final String field) {
    //     unsetInEntity(toSet, field);
    // }

    public <T> void unsetInEntity(final T toSet, final String field) {
        // noinspection unchecked
        unsetInEntity(toSet, field, (AsyncOperationCallback) null);
    }

    /**
     * This method unsets a property.
     *
     * Please use {@link Morphium#unsetInEntity(Object, Enum, AsyncOperationCallback)} instead.
     */
    // @Deprecated
    // public <T> void unset(final T toSet, final Enum<?> field, final AsyncOperationCallback<T> callback) {
    //     unsetInEntity(toSet, field, callback);
    // }

    public <T> void unsetInEntity(final T toSet, final Enum<?> field, final AsyncOperationCallback<T> callback) {
        unsetInEntity(toSet, field.name(), callback);
    }

    /**
     * This method unsets a property.
     *
     * Please use {@link Morphium#unsetInEntity(Object, String, Enum)} instead.
     */
    // @Deprecated
    // public <T> void unset(final T toSet, String collection, final Enum<?> field) {
    //     unsetInEntity(toSet, collection, field);
    // }
    public <T> void unsetInEntity(final T toSet, String collection, final Enum<?> field) {
        unsetInEntity(toSet, collection, field.name(), null);
    }

    /**
     * This method unsets a property.
     *
     */
    // @Deprecated
    // @SuppressWarnings("unused")
    // public <T> void unset(final T toSet, String collection, final Enum<?> field, final AsyncOperationCallback<T> callback) {
    //     unsetInEntity(toSet, collection, field.name(), callback);
    // }
    public <T> void unsetInEntity(final T toSet, String collection, final Enum<?> field, final AsyncOperationCallback<T> callback) {
        unsetInEntity(toSet, collection, field.name(), callback);
    }

    /**
     * This method unsets a property.
     *
     */
    // @Deprecated
    // public <T> void unset(final T toSet, final String field, final AsyncOperationCallback<T> callback) {
    //     unsetInEntity(toSet, getMapper().getCollectionName(toSet.getClass()), field, callback);
    // }

    public <T> void unsetInEntity(final T toSet, final String field, final AsyncOperationCallback<T> callback) {
        unsetInEntity(toSet, getMapper().getCollectionName(toSet.getClass()), field, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Map<String, Number> toUpdate, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, toUpdate, upsert, multiple, callback);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final long amount, final boolean upsert, final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final int amount, final boolean upsert, final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final double amount, final boolean upsert, final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public Map<String, Object> inc(final Query<?> query, final String name, final Number amount, final boolean upsert, final boolean multiple) {
        return inc(query, name, amount, upsert, multiple, null);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final Enum<?> field, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return inc(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> inc(final Query<T> query, final String name, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final long amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final int amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final double amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final Enum<?> field, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        return dec(query, field.name(), amount, upsert, multiple, callback);
    }

    public <T> Map<String, Object> dec(final Query<T> query, final String name, final Number amount, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).inc(query, name, -amount.doubleValue(), upsert, multiple, callback);
    }

    /**
     * updating an enty in DB without sending the whole entity only transfers the
     * fields to be
     * changed / set
     *
     * @param ent - entity to update
     * @param fields - fields to use
     */
    public void updateUsingFields(final Object ent, final String... fields) {
        updateUsingFields(ent, null, fields);
    }

    public void updateUsingFields(final Object ent, final Enum... fieldNames) {
        updateUsingFields(ent, null, fieldNames);
    }

    public <T> void updateUsingFields(final T ent, AsyncOperationCallback<T> callback, final Enum... fields) {
        updateUsingFields(ent, getMapper().getCollectionName(ent.getClass()), callback, fields);
    }

    public <T> void updateUsingFields(final T ent, AsyncOperationCallback<T> callback, final String... fields) {
        updateUsingFields(ent, getMapper().getCollectionName(ent.getClass()), callback, fields);
    }

    public <T> void updateUsingFields(final T ent, String collection, AsyncOperationCallback<T> callback, final Enum... fields) {
        List<String> g = new ArrayList<>();

        for (Enum<?> e : fields) {
            g.add(e.name());
        }

        updateUsingFields(ent, collection, callback, g.toArray(new String[] {}));
    }

    @SuppressWarnings({"UnusedParameters", "CommentedOutCode"})
    public <T> void updateUsingFields(final T ent, String collection, AsyncOperationCallback<T> callback, final String... fields) {
        if (ent == null) {
            return;
        }

        if (fields.length == 0) {
            return; // not doing an update - no change
        }

        for (int idx = 0; idx < fields.length; idx++) {
            fields[idx] = getARHelper().getMongoFieldName(ent.getClass(), fields[idx]);
        }

        getWriterForClass(ent.getClass()).updateUsingFields(ent, collection, null, fields);
    }

    public <T> void dec(final Map<Enum, Number> fieldsToInc, final Query<T> matching, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Number> toUpdate = new HashMap<>();

        for (Map.Entry<Enum, Number> e : fieldsToInc.entrySet()) {
            toUpdate.put(e.getKey().name(), e.getValue());
        }

        dec(matching, toUpdate, upsert, multiple, callback);
    }

    /**
     * decreasing a value of a given object calles
     * <code>inc(toDec,field,-amount);</code>
     */
    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum<?> field, double amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, double amount) {
        inc(toDec, field, -amount);
    }

    public void dec(Object toDec, Enum<?> field, int amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, int amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum<?> field, long amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, long amount) {
        inc(toDec, field, -amount);
    }

    @SuppressWarnings("unused")
    public void dec(Object toDec, Enum<?> field, Number amount) {
        dec(toDec, field.name(), amount);
    }

    public void dec(Object toDec, String field, Number amount) {
        inc(toDec, field, -amount.doubleValue());
    }

    public void inc(final Object toSet, final Enum<?> field, final long i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final long i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final Enum<?> field, final int i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final int i) {
        inc(toSet, field, i, null);
    }

    public void inc(final Object toSet, final Enum<?> field, final double i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final double i) {
        inc(toSet, field, i, null);
    }

    @SuppressWarnings("unused")
    public void inc(final Object toSet, final Enum<?> field, final Number i) {
        inc(toSet, field.name(), i, null);
    }

    public void inc(final Object toSet, final String field, final Number i) {
        inc(toSet, field, i, null);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, final Enum<?> field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, final String field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, getMapper().getCollectionName(toSet.getClass()), field, i, callback);
    }

    public <T> void inc(final T toSet, Enum<?> collection, final Enum<?> field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final double i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final double i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final int i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final int i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final long i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final long i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void inc(final T toSet, String collection, final Enum<?> field, final Number i, final AsyncOperationCallback<T> callback) {
        inc(toSet, collection, field.name(), i, callback);
    }

    public <T> void inc(final T toSet, String collection, final String field, final Number i, final AsyncOperationCallback<T> callback) {
        if (toSet == null) {
            throw new RuntimeException("Cannot update null!");
        }

        if (getId(toSet) == null) {
            log.debug("just storing object as it is new...");
            store(toSet);
            return;
        }

        getWriterForClass(toSet.getClass()).inc(toSet, collection, field, i, callback);
    }

    public <T> void remove(List<T> lst, String forceCollectionName) {
        remove(lst, forceCollectionName, (AsyncOperationCallback<T>) null);
    }

    public <T> Map<String, Object> delete (Query<T> o) {
        return remove(o);
    }

    public <T> Map<String, Object> explainRemove(Query<T> q) {
        return getConfig().writerSettings().getWriter().explainRemove(null, q);
    }

    public <T> Map<String, Object> remove(Query<T> o) {
        return getWriterForClass(o.getType()).remove(o, null);
    }

    public <T> Map<String, Object> delete (Query<T> o, final AsyncOperationCallback<T> callback) {
        return remove(o, callback);
    }

    public <T> Map<String, Object> remove(Query<T> o, final AsyncOperationCallback<T> callback) {
        return getWriterForClass(o.getType()).remove(o, callback);
    }

    public <T> Map<String, Object> pushPull(boolean push, Query<T> query, String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        return getWriterForClass(query.getType()).pushPull(push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }

    public <T> Map<String, Object> pushPullAll(boolean push, Query<T> query, String field, List<?> value, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        return getWriterForClass(query.getType()).pushPullAll(push ? UpdateTypes.PUSH : UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }

    public <T> Map<String, Object> pullAll(Query<T> query, String field, List<?> value, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        return getWriterForClass(query.getType()).pushPullAll(UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }

    /**
     * deletes a single object from morphium backend. Clears cache
     *
     * @param o - entity
     */
    public void remove(Object o) {
        remove(o, getMapper().getCollectionName(o.getClass()));
    }

    public void delete (Object o) {
        remove(o, getMapper().getCollectionName(o.getClass()));
    }

    public void remove(Object o, String collection) {
        getWriterForClass(o.getClass()).remove(o, collection, null);
    }

    public void delete (Object o, String collection) {
        remove(o, collection);
    }

    public <T> void delete (final T lo, final AsyncOperationCallback<T> callback) {
        remove(lo, callback);
    }

    public <T> void remove(List<T> lst, String forceCollectionName, AsyncOperationCallback<T> callback) {
        ArrayList<T> directDel = new ArrayList<>();
        ArrayList<T> bufferedDel = new ArrayList<>();

        for (T o : lst) {
            if (getARHelper().isBufferedWrite(o.getClass())) {
                bufferedDel.add(o);
            } else {
                directDel.add(o);
            }
        }

        for (T o : bufferedDel) {
            getConfig().writerSettings().getBufferedWriter().remove(o, forceCollectionName, callback);
        }

        for (T o : directDel) {
            getConfig().writerSettings().getWriter().remove(o, forceCollectionName, callback);
        }
    }

    @SuppressWarnings("unused")
    public <T> void delete (List<T> lst, AsyncOperationCallback<T> callback) {
        remove(lst, callback);
    }

    public <T> void remove(List<T> lst, AsyncOperationCallback<T> callback) {
        ArrayList<T> directDel = new ArrayList<>();
        ArrayList<T> bufferedDel = new ArrayList<>();

        for (T o : lst) {
            if (getARHelper().isBufferedWrite(o.getClass())) {
                bufferedDel.add(o);
            } else {
                directDel.add(o);
            }
        }

        getConfig().writerSettings().getBufferedWriter().remove(bufferedDel, callback);
        getConfig().writerSettings().getWriter().remove(directDel, callback);
    }

    public <T> void remove(final T lo, final AsyncOperationCallback<T> callback) {
        if (lo instanceof Query) {
            // noinspection unchecked
            remove((Query) lo, callback);
            return;
        }

        getWriterForClass(lo.getClass()).remove(lo, getMapper().getCollectionName(lo.getClass()), callback);
    }

    @SuppressWarnings("unused")
    public <T> void delete (final T lo, String collection, final AsyncOperationCallback<T> callback) {
        remove(lo, collection, callback);
    }

    public <T> void remove(final T lo, String collection, final AsyncOperationCallback<T> callback) {
        getWriterForClass(lo.getClass()).remove(lo, collection, callback);
    }

    public boolean exists(String db) throws MorphiumDriverException {
        MongoConnection primaryConnection = getDriver().getPrimaryConnection(null);
        ListDatabasesCommand cmd = new ListDatabasesCommand(primaryConnection);
        var dbs = cmd.getList();
        cmd.releaseConnection();

        // var ret = getDriver().runCommand("admin", Doc.of("listDatabasess", 1));
        for (Map<String, Object> l : dbs) {
            if (l.get("name").equals(db)) {
                return true;
            }
        }

        return false;
    }

    public boolean exists(String db, String col) throws MorphiumDriverException {
        return getDriver().listCollections(db, col).size() != 0;
    }

    public boolean exists(Class<?> cls) throws MorphiumDriverException {
        return exists(getDatabase(), getMapper().getCollectionName(cls));
    }

    /**
     * sorts elements in this list, whether to store in background or directly.
     *
     * @param lst - all objects are sorted whether to store in BG or direclty. All
     *        objects are
     *        stored in their corresponding collection
     * @param <T> - type of list elements
     */
    public <T> void storeList(List<T> lst) {
        saveList(lst);
    }

    public <T> void saveList(List<T> lst) {
        saveList(lst, (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(Set<T> set) {
        saveList(set);
    }

    public <T> void saveList(Set<T> set) {
        saveList(new ArrayList<>(set), (AsyncOperationCallback<T>) null);
    }

    public <T> void storeList(List<T> lst, final AsyncOperationCallback<T> callback) {
        saveList(lst, callback);
    }

    /**
     * stores all elements of this list to the given collection
     *
     * @param lst - list of objects to store
     * @param collection - collection name to use
     * @param <T> - type of entity
     */
    public <T> void storeList(List<T> lst, String collection) {
        saveList(lst, collection, null);
    }

    public <T> void saveList(List<T> lst, String collection) {
        saveList(lst, collection, null);
    }

    public <T> void storeList(List<T> lst, String collection, AsyncOperationCallback<T> callback) {
        saveList(lst, collection, callback);
    }
    /**
     * Stores a single Object. Clears the corresponding cache
     *
     * @param o - Object to store
     */
    public <T> void store(T o) {
        save(o);
    }

    public <T> void save(T o) {
        if (o instanceof List) {
            // noinspection unchecked
            saveList((List) o);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            saveList(new ArrayList<>((Collection) o));
        } else {
            save(o, getMapper().getCollectionName(o.getClass()), null);
        }
    }

    public <T> void store(T o, final AsyncOperationCallback<T> callback) {
        save(o, callback);
    }

    public <T> void save(T o, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            // noinspection unchecked
            saveList((List) o, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            saveList(new ArrayList<>((Collection) o), callback);
        } else {
            save(o, getMapper().getCollectionName(o.getClass()), callback);
        }
    }

    public <T> void store(T o, String collection, final AsyncOperationCallback<T> callback) {
        save(o, collection, callback);
    }

    public <T> void save(T o, String collection, final AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            // noinspection unchecked
            saveList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            saveList(new ArrayList<>((Collection) o), collection, callback);
        }

        if (getARHelper().getId(o) != null) {
            getWriterForClass(o.getClass()).store(o, collection, callback);
        } else {
            getWriterForClass(o.getClass()).insert(o, collection, callback);
        }
    }

    ////////
    /////
    //
    // SET with object
    //
    @SuppressWarnings("unused")
    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object, Enum, Object, AsyncOperationCallback)} instead.
     */
    // @Deprecated
    // public <T> void set(T toSet, Enum<?> field, Object value, AsyncOperationCallback<T> callback) {
    //     setInEntity(toSet, field, value, callback);
    // }

    public <T> void setInEntity(T toSet, Enum<?> field, Object value, AsyncOperationCallback<T> callback) {
        setInEntity(toSet, field.name(), value, callback);
    }

    /**
     * This method sets a property.
     *
     * Please use {@link Query#setInEntity(Object,Enum,Object)} instead.
     */
    // @Deprecated
    // public <T> void set(T toSet, Enum<?> field, Object value) {
    //     setInEntity(toSet, field, value);
    // }

    public <T> void setInEntity(T toSet, Enum<?> field, Object value) {
        setInEntity(toSet, field.name(), value, null);
    }

    public <T> void setInEntity(T toSet, String collection, Enum<?> field, Object value) {
        setInEntity(toSet, collection, field.name(), value, false, null);
    }

    /**
     * This method sets a property.
     *
     * Please use {@link Morphium#setInEntity(Object, Enum, Object, boolean, AsyncOperationCallback)} instead.
     */
    public <T> void setInEntity(final T toSet, final Enum<?> field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        setInEntity(toSet, field.name(), value, upserts, callback);
    }

    /**
     * This method sets properties.
     *
     */
    public <T> void setInEntity(final T toSet, final Map<Enum, Object> values) {
        setInEntity(toSet, getMapper().getCollectionName(toSet.getClass()), false, values, null);
    }



    /**
     * This method sets properties.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object,String, boolean, Map)} instead.
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values) {
        set(toSet, collection, upserts, values, null);
    }
    public <T> void setInEntity(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values) {
        set(toSet, collection, upserts, values, null);
    }

    public <T> Map<String, Object> explainRemove(ExplainVerbosity verbosity, T o) {
        return getConfig().writerSettings().getWriter().explainRemove(verbosity, o, getMapper().getCollectionName(o.getClass()));
    }

    /**
     * setting a value in an existing mongo collection entry - no reading necessary.
     * Object is
     * altered in place db.collection.update({"_id":toSet.id},{$set:{field:value}}
     * <b>attention</b>:
     * this alteres the given object toSet in a similar way
     *
     * @param toSet - object to set the value in (or better - the corresponding
     *              entry in mongo)
     * @param field - the field to change
     * @param value - the value to set
     *              *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(String, Object, String, Object)} instead.
     */
    @Deprecated
    public <T> void set(final T toSet, final String field, final Object value) {
        set(toSet, field, value, null);
    }

    public <T> void setInEntity(final T toSet, final String field, final Object value) {
        set(toSet, field, value, null);
    }


    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object,String, Object, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> void set(final T toSet, final String field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, getMapper().getCollectionName(toSet.getClass()), field, value, upserts, callback);
    }

    public <T> void setInEntity(final T toSet, final String field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        setInEntity(toSet, getMapper().getCollectionName(toSet.getClass()), field, value, upserts, callback);
    }


    /**
     * This method sets properties.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object, String, Map)
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, final Map<String, Object> values) {
        set(toSet, collection, values, false, null);
    }

    public <T> void setInEntity(final T toSet, String collection, final Map<String, Object> values) {
        set(toSet, collection, values, false, null);
    }

    /**
     * This method sets properties.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object, String,Map,boolean)} instead.
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, final Map<String, Object> values, boolean upserts) {
        set(toSet, collection, values, upserts, null);
    }
    public <T> void setInEntity(final T toSet, String collection, final Map<String, Object> values, boolean upserts) {
        set(toSet, collection, values, upserts, null);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object,String,Enum,Object, boolean, AsyncOperationCallback)} instead.
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, final Enum field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, collection, UtilsMap.of(field.name(), value), upserts, callback);
    }
    public <T> void setInEntity(final T toSet, String collection, final Enum field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, collection, UtilsMap.of(field.name(), value), upserts, callback);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object, String, String,Object, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> void set(final T toSet, String collection, final String field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        set(toSet, collection, UtilsMap.of(field, value), upserts, callback);
    }
    public <T> void setInEntity(final T toSet, String collection, final String field, final Object value, boolean upserts, AsyncOperationCallback<T> callback) {
        setInEntity(toSet, collection, UtilsMap.of(field, value), upserts, callback);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Morphium#setInEntity(Object,String, Object, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> void set(final T toSet, final String field, final Object value, final AsyncOperationCallback<T> callback) {
        set(toSet, field, value, false, callback);
    }
    public <T> void setInEntity(final T toSet, final String field, final Object value, final AsyncOperationCallback<T> callback) {
        setInEntity(toSet, field, value, false, callback);
    }
    ///////////////////////////////
    //////////////////////
    ///////////////
    ////////////
    ////////// DEC and INC Methods
    /////
    //

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> dec(Query<?> query, Enum<?> field, double amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, long amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, Number amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, int amount, boolean upsert, boolean multiple) {
        return dec(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> dec(Query<?> query, String field, double amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount, upsert, multiple);
    }

    public Map<String, Object> dec(Query<?> query, String field, long amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount, upsert, multiple);
    }

    public Map<String, Object> dec(Query<?> query, String field, int amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, Number amount, boolean upsert, boolean multiple) {
        return inc(query, field, -amount.doubleValue(), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, double amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, long amount) {
        return inc(query, field, -amount, false, false);
    }

    public Map<String, Object> dec(Query<?> query, String field, int amount) {
        return inc(query, field, -amount, false, false, null);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, String field, Number amount) {
        return inc(query, field, -amount.doubleValue(), false, false);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> dec(Query<?> query, Enum<?> field, double amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, long amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, int amount) {
        return inc(query, field, -amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> dec(Query<?> query, Enum<?> field, Number amount) {
        return inc(query, field, -amount.doubleValue(), false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, String field, long amount) {
        return inc(query, field, amount, false, false);
    }

    public Map<String, Object> inc(Query<?> query, String field, int amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, String field, Number amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, String field, double amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> inc(Query<?> query, Enum<?> field, double amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, Enum<?> field, long amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, Enum<?> field, int amount) {
        return inc(query, field, amount, false, false);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> inc(Query<?> query, Enum<?> field, Number amount) {
        return inc(query, field, amount, false, false);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, double amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, long amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, int amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public Map<String, Object> inc(Query<?> query, Enum<?> field, Number amount, boolean upsert, boolean multiple) {
        return inc(query, field.name(), amount, upsert, multiple);
    }

    public <T> T findById(Class <? extends T > type, Object id) {
        return findById(type, id, null);
    }

    public List<Object> distinct(String key, String collectionName) {
        return distinct(key, collectionName, null);
    }

    @SuppressWarnings("unused")
    public List<Object> distinct(Enum<?> key, Class c) {
        return distinct(key.name(), c);
    }

    /**
     * returns a distinct list of values of the given collection Attention: these
     * values are not
     * unmarshalled, you might get MongoMap<String,Object>s
     */
    @SuppressWarnings("unused")
    public List<Object> distinct(Enum<?> key, Query q) {
        return distinct(key.name(), q);
    }

    public List<Object> distinct(String key, Class cls) {
        return distinct(key, cls, null);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value) {
        return addToSet(query, field, value, false, false);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value, final boolean multiple) {
        return addToSet(query, field, value, false, multiple);
    }

    public <T> Map<String, Object> addToSet(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPull(UpdateTypes.ADD_TO_SET, query, field, value, upsert, multiple, null);
    }

    public void push(Object entity, String collection, Enum<?> field, Object value, boolean upsert) {
        push(entity, collection, field.name(), value, upsert);
    }

    public void push(Object entity, Enum<?> field, Object value, boolean upsert) {
        push(entity, field.name(), value, upsert);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> pullAll(Query<?> query, Enum<?> field, List<Object> value, boolean upsert, boolean multiple) {
        return pull(query, field.name(), value, upsert, multiple, null);
    }

    public <T> Map<String, Object> push(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple) {
        return push(query, field, value, upsert, multiple, null);
    }
    public <T> void save(List<T> lst, AsyncOperationCallback<T> callback) {
        saveList(lst, callback);
    }

    public <T> void store(List<T> lst, AsyncOperationCallback<T> callback) {
        saveList(lst, callback);
    }


    /**
     * Asynchronous call to pulll
     *
     * @param query - query
     * @param field - field to pull
     * @param value - value to pull from field
     * @param upsert - insert document unless it exists
     * @param multiple - more than one
     * @param callback -callback to call when operation succeeds - synchronous call,
     *        if null
     * @param <T> - type
     */
    public <T> Map<String, Object> pull(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPull(MorphiumStorageListener.UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }


    public Map<String, Object> push(final Query<?> query, final Enum<?> field, final Object value) {
        return push(query, field, value, false, true);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> pull(Query<?> query, Enum<?> field, Object value) {
        return pull(query, field.name(), value, false, true, null);
    }

    public Map<String, Object> push(Query<?> query, String field, Object value) {
        return push(query, field, value, false, true);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> pull(Query<?> query, String field, Object value) {
        return pull(query, field, value, false, true, null);
    }

    public Map<String, Object> push(Query<?> query, Enum<?> field, Object value, boolean upsert, boolean multiple) {
        return push(query, field.name(), value, upsert, multiple);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> pull(Query<?> query, Enum<?> field, Object value, boolean upsert, boolean multiple) {
        return pull(query, field.name(), value, upsert, multiple, null);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> pushAll(Query<?> query, Enum<?> field, List<Object> value, boolean upsert, boolean multiple) {
        return pushAll(query, field.name(), value, upsert, multiple);
    }

    public void push(Object entity, String field, Object value, boolean upsert) {
        push(entity, getMapper().getCollectionName(entity.getClass()), field, value, upsert);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(Enum, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val) {
        return set(query, field, val, (AsyncOperationCallback<T>) null);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(String, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val, AsyncOperationCallback<T> callback) {
        Map<String, Object> toSet = new HashMap<>();
        toSet.put(field.name(), val);
        return getWriterForClass(query.getType()).set(query, toSet, false, false, callback);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(String, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, String field, Object val) {
        return set(query, field, val, (AsyncOperationCallback<T>) null);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(String, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, String field, Object val, AsyncOperationCallback<T> callback) {
        Map<String, Object> toSet = new HashMap<>();
        toSet.put(field, val);
        return getWriterForClass(query.getType()).set(query, toSet, false, false, callback);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> setEnum(Query<?> query, Map<Enum, Object> values, boolean upsert, boolean multiple) {
        HashMap<String, Object> toSet = new HashMap<>();

        for (Map.Entry<Enum, Object> est : values.entrySet()) {
            // noinspection SuspiciousMethodCalls
            toSet.put(est.getKey().name(), values.get(est.getValue()));
        }

        return set(query, toSet, upsert, multiple);
    }
    public <T> Map<String, Object> pushAll(final Query<T> query, final String field, final List<?> value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPullAll(UpdateTypes.PUSH, query, field, value, upsert, multiple, callback);
    }

    public <T> Map<String, Object> addAllToSet(final Query<T> query, final String field, final List<?> value, final boolean multiple) {
        return addAllToSet(query, field, value, false, multiple, null);
    }

    public <T> Map<String, Object> addAllToSet(final Query<T> query, final String field, final List<?> value, final boolean upsert, final boolean multiple) {
        return addAllToSet(query, field, value, upsert, multiple, null);
    }

    public <T> Map<String, Object> addAllToSet(final Query<T> query, final String field, final List<?> value, final boolean upsert, final boolean multiple, final AsyncOperationCallback callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPullAll(UpdateTypes.ADD_TO_SET, query, field, value, upsert, multiple, callback);
    }


    public <T> Map<String, Object> pull(final Query<T> query, final String field, final Expr value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback) {
        if (query == null || field == null) {
            throw new RuntimeException("Cannot update null!");
        }

        MorphiumWriter wr = getWriterForClass(query.getType());
        return wr.pushPull(MorphiumStorageListener.UpdateTypes.PULL, query, field, value, upsert, multiple, callback);
    }

    public Map<String, Object> pushAll(final Query<?> query, final String field, final List<?> value, final boolean upsert, final boolean multiple) {
        return pushAll(query, field, value, upsert, multiple, null);
    }



    /**
     * will change an entry in mongodb-collection corresponding to given class
     * object if query is
     * too complex, upsert might not work! Upsert should consist of single anueries,
     * which will be
     * used to generate the object to create, unless it already exists. look at
     * Mongodb-query
     * documentation as well
     *
     * @param query - query to specify which objects should be set
     * @param field - field to set
     * @param val - value to set
     * @param upsert - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be
     *        updated
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(Enum, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    @SuppressWarnings("unused")
    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val, boolean upsert, boolean multiple) {
        return set(query, field.name(), val, upsert, multiple, null);
    }

    @SuppressWarnings("unused")
    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(Enum, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, Enum<?> field, Object val, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        return set(query, field.name(), val, upsert, multiple, callback);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Map<String, Object> pullAll(Query<?> query, String field, List<Object> value, boolean upsert, boolean multiple) {
        return pull(query, field, value, upsert, multiple, null);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(String, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, String field, Object val, boolean upsert, boolean multiple) {
        return set(query, field, val, upsert, multiple, null);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(String, Object, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(Query<T> query, String field, Object val, boolean upsert, boolean multiple, AsyncOperationCallback<T> callback) {
        Map<String, Object> map = new HashMap<>();
        map.put(field, val);
        return set(query, map, upsert, multiple, callback);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(Map, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public Map<String, Object> set(final Query<?> query, final Map<String, Object> map, final boolean upsert, final boolean multiple) {
        return set(query, map, upsert, multiple, null);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#set(Map, boolean, boolean, AsyncOperationCallback)}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> set(final Query<T> query, final Map<String, Object> map, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback) {
        if (query == null) {
            throw new RuntimeException("Cannot update null!");
        }

        return getWriterForClass(query.getType()).set(query, map, upsert, multiple, callback);
    }

    /**
     * set current date into one field
     *
     * @param query
     * @param field
     * @param upsert
     * @param multiple
     * @param <T>
     */
    public <T> Map<String, Object> currentDate(final Query<?> query, String field, boolean upsert, boolean multiple) {
        return set(query, UtilsMap.of("$currentDate", UtilsMap.of(field, 1)), upsert, multiple);
    }

    public <T> Map<String, Object> currentDate(final Query<?> query, Enum field, boolean upsert, boolean multiple) {
        return set(query, UtilsMap.of("$currentDate", UtilsMap.of(field.name(), 1)), upsert, multiple);
    }

    public <T> Map<String, Object> pull(final T entity, final String field, final Expr value, final boolean upsert, final boolean multiple) {
        return pull(entity, field, value, upsert, multiple, null);
    }
    public abstract <T> Map<String, Object> pull(final T entity, final String field, final Expr value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback);
    public abstract void push(Object entity, String collection, String field, Object value, boolean upsert);
    // public abstract void push(Object entity, String collection, String field, Object value, boolean upsert);
    public abstract <T> Map<String, Object> push(final Query<T> query, final String field, final Object value, final boolean upsert, final boolean multiple, final AsyncOperationCallback<T> callback);



    public <T> void insertList(List arrayList, AsyncOperationCallback<T> callback) {
        insertList(arrayList, null, callback);
    }

    public <T> void insertList(List arrayList) {
        insertList(arrayList, null, null);
    }
    public <T> void insert(T o) {
        if (o instanceof List) {
            insertList((List) o, null);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), null);
        } else {
            insert(o, null);
        }
    }

    public <T> void insert(T o, AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            insertList((List) o, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), callback);
        } else {
            insert(o, getMapper().getCollectionName(o.getClass()), callback);
        }
    }

    public <T> void insert(T o, String collection, AsyncOperationCallback<T> callback) {
        if (o instanceof List) {
            insertList((List) o, collection, callback);
        } else if (o instanceof Collection) {
            // noinspection unchecked
            insertList(new ArrayList<>((Collection) o), collection, callback);
        } else {
            getWriterForClass(o.getClass()).insert(o, collection, callback);
        }
    }
    /**
     * directly writes data to Mongo, no Mapper used use with caution, as caches are
     * not updated
     * also no checks for validity of fields, no references, no auto-variables no
     * async writing!
     *
     * @param type - type to write to (just for determining collection name)
     * @param lst - list of entries to write
     * @return statistics
     * @throws MorphiumDriverException
     */
    public Map<String, Integer> storeMaps(Class type, List<Map<String, Object >> lst) throws MorphiumDriverException {
        return saveMaps(type, lst);
    }

    public <T> void storeNoCache(T lst) {
        storeNoCache(lst, getMapper().getCollectionName(lst.getClass()), null);
    }

    @SuppressWarnings("unused")
    public <T> void storeNoCache(T o, AsyncOperationCallback<T> callback) {
        storeNoCache(o, getMapper().getCollectionName(o.getClass()), callback);
    }

    public <T> void storeNoCache(T o, String collection) {
        storeNoCache(o, collection, null);
    }

    public <T> void storeNoCache(T o, String collection, AsyncOperationCallback<T> callback) {
        if (getARHelper().getId(o) == null) {
            getConfig().getWriter().insert(o, collection, callback);
        } else {
            getConfig().getWriter().store(o, collection, callback);
        }
    }

    public <T> void storeBuffered(final T lst) {
        storeBuffered(lst, null);
    }

    public <T> void storeBuffered(final T lst, final AsyncOperationCallback<T> callback) {
        storeBuffered(lst, getMapper().getCollectionName(lst.getClass()), callback);
    }

    public <T> void storeBuffered(final T lst, String collection, final AsyncOperationCallback<T> callback) {
        getConfig().getBufferedWriter().store(lst, collection, callback);
    }


    /**
     * can be called for autmatic index ensurance. Attention: might cause heavy load
     * on mongo
     *
     * @param type type to ensure indices for
     */
    public <T> void ensureIndicesFor(Class<T> type) {
        ensureIndicesFor(type, getMapper().getCollectionName(type), null);
    }

    public <T> void ensureIndicesFor(Class<T> type, String onCollection) {
        ensureIndicesFor(type, onCollection, null);
    }

    public <T> void ensureIndicesFor(Class<T> type, AsyncOperationCallback<T> callback) {
        ensureIndicesFor(type, getMapper().getCollectionName(type), callback);
    }

    public <T> void ensureIndicesFor(Class<T> type, String onCollection, AsyncOperationCallback<T> callback) {
        ensureIndicesFor(type, onCollection, callback, getWriterForClass(type));
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#unset(String[])}  instead.
     */
    @Deprecated
    public <T> Map<String, Object> unsetQ(Query<T> q, String... field) {
        return getWriterForClass(q.getType()).unset(q, null, false, field);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#unset(boolean, String[])} instead.
     */
    @Deprecated
    public <T> Map<String, Object> unsetQ(Query<T> q, boolean multiple, String... field) {
        return getWriterForClass(q.getType()).unset(q, null, multiple, field);
    }

    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#unset(Enum[])} instead.
     */
    @Deprecated
    public <T> Map<String, Object> unsetQ(Query<T> q, Enum... field) {
        return getWriterForClass(q.getType()).unset(q, null, false, field);
    }


    /**
     * This method sets a property.
     *
     * @deprecated There is a newer implementation.
     * Please use {@link Query#unset(boolean, Enum[])} instead.
     */
    @Deprecated
    public <T> Map<String, Object> unsetQ(Query<T> q, boolean multiple, Enum... field) {
        return getWriterForClass(q.getType()).unset(q, null, multiple, field);
    }


    public <T> Map<String, Object> unsetQ(Query<T> q, AsyncOperationCallback<T> cb, String... field) {
        return getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    @SuppressWarnings({"unused", "UnusedParameters"})
    public <T> Map<String, Object> unsetQ(Query<T> q, AsyncOperationCallback<T> cb, boolean multiple, String ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, AsyncOperationCallback<T> cb, Enum ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, false, field);
    }

    public <T> Map<String, Object> unsetQ(Query<T> q, boolean multiple, AsyncOperationCallback<T> cb, Enum ... field) {
        return getWriterForClass(q.getType()).unset(q, cb, multiple, field);
    }

    public abstract <T> void ensureIndicesFor(Class<T> type, String onCollection, AsyncOperationCallback<T> callback, MorphiumWriter wr);
    public abstract Map<String, Integer> saveMaps(Class type, List<Map<String, Object >> lst) throws MorphiumDriverException;
    public abstract <T> void insertList(List lst, String collection, AsyncOperationCallback<T> callback);
    public abstract <T> T findById(Class <? extends T > type, Object id, String collection);

    public abstract <T> void findById(Class <? extends T > type, Object id, String collection, AsyncOperationCallback callback);

    public abstract List<Object> distinct(String key, Query q);

    public abstract List<Object> distinct(String key, String collectionName, Collation collation);

    public abstract List<Object> distinct(String key, Class cls, Collation collation);

    public abstract <T> Map<String, Object> inc(final Map<Enum, Number> fieldsToInc, final Query<T> matching, final boolean upsert, final boolean multiple, AsyncOperationCallback<T> callback);

    public abstract <T> void setInEntity(final T toSet, String collection, final Map<String, Object> values, boolean upserts, AsyncOperationCallback<T> callback);
    public abstract <T> void set(final T toSet, String collection, final Map<String, Object> values, boolean upserts, AsyncOperationCallback<T> callback);

    public abstract <T> void set(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values, AsyncOperationCallback<T> callback);
    public abstract <T> void setInEntity(final T toSet, String collection, boolean upserts, final Map<Enum, Object> values, AsyncOperationCallback<T> callback);
    // public abstract <T> void unset(final T toSet, String collection, final String field, final AsyncOperationCallback<T> callback);
    public abstract <T> void unsetInEntity(final T toSet, String collection, final String field, final AsyncOperationCallback<T> callback);

    public abstract boolean isWriteBufferEnabledForThread();

    public abstract MorphiumDriver getDriver();

    public abstract String getDatabase();


    public abstract Object getId(Object o);


    public abstract MorphiumWriter getWriterForClass(Class<?> cls);

    public abstract MorphiumObjectMapper getMapper();

    public abstract AnnotationAndReflectionHelper getARHelper();

    public abstract MorphiumConfig getConfig();

    public abstract <T> void saveList(List<T> lst, String collection, AsyncOperationCallback<T> callback);
    public abstract <T> void saveList(List<T> lst, final AsyncOperationCallback<T> callback);

}
