package de.caluga.morphium.writer;

import com.mongodb.*;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 14:38
 * <p/>
 * default writer implementation - uses a ThreadPoolExecutor for execution of asynchornous calls
 * maximum Threads are limited to 0.9* MaxConnections configured in MorphiumConfig
 *
 * @see MorphiumWriter
 */
@SuppressWarnings({"ConstantConditions", "unchecked"})
public class MorphiumWriterImpl implements MorphiumWriter {
    private static Logger logger = Logger.getLogger(MorphiumWriterImpl.class);
    private Morphium morphium;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();

    private ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        if (m != null) {
            annotationHelper = morphium.getARHelper();
            executor.setCorePoolSize(m.getConfig().getMaxConnections() / 2);
            executor.setMaximumPoolSize((int) (m.getConfig().getMaxConnections() * m.getConfig().getBlockingThreadsMultiplier() * 0.9));
        } else {
            annotationHelper = new AnnotationAndReflectionHelper();
        }
    }


    /**
     * @param obj - object to store
     */
    @Override
    public <T> void store(final T obj, final String collection, final AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            store((List) obj, callback);
            return;
        }
        Runnable r = new Runnable() {
            public void run() {
                long start = System.currentTimeMillis();

                try {
                    T o = obj;
                    Class type = annotationHelper.getRealClass(o.getClass());
                    if (!annotationHelper.isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }
                    morphium.inc(StatisticKeys.WRITES);
                    ObjectId id = annotationHelper.getId(o);
                    if (annotationHelper.isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
                        if ((o instanceof PartiallyUpdateable)) {
                            updateUsingFields(o, collection, callback, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
                            ((PartiallyUpdateable) o).clearAlteredFields();

                            return;
                        }
                    }
                    o = annotationHelper.getRealObject(o);
                    if (o == null) {
                        logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                        return;
                    }
                    boolean isNew = id == null;
                    morphium.firePreStoreEvent(o, isNew);

                    DBObject marshall = morphium.getMapper().marshall(o);

                    if (isNew) {
                        //new object - need to store creation time
                        if (annotationHelper.isAnnotationPresentInHierarchy(type, CreationTime.class)) {
                            List<String> lst = annotationHelper.getFields(type, CreationTime.class);
                            if (lst == null || lst.size() == 0) {
                                logger.error("Unable to store creation time as @CreationTime is missing");
                            } else {
                                long now = System.currentTimeMillis();
                                for (String ctf : lst) {
                                    Field f = annotationHelper.getField(type, ctf);
                                    if (f != null) {
                                        try {
                                            f.set(o, now);
                                        } catch (IllegalAccessException e) {
                                            logger.error("Could not set creation time", e);

                                        }
                                    }
                                    marshall.put(ctf, now);
                                }

                            }

                        }
                    }
                    if (annotationHelper.isAnnotationPresentInHierarchy(type, LastChange.class)) {
                        List<String> lst = annotationHelper.getFields(type, LastChange.class);
                        if (lst != null && lst.size() > 0) {
                            for (String ctf : lst) {
                                long now = System.currentTimeMillis();
                                Field f = annotationHelper.getField(type, ctf);
                                if (f != null) {
                                    try {
                                        f.set(o, now);
                                    } catch (IllegalAccessException e) {
                                        logger.error("Could not set modification time", e);

                                    }
                                }
                                marshall.put(ctf, now);
                            }
                        } else {
                            logger.warn("Could not store last change - @LastChange missing!");
                        }

                    }

                    String coll = collection;
                    if (coll == null) {
                        coll = morphium.getMapper().getCollectionName(type);
                    }
                    if (!morphium.getDatabase().collectionExists(coll)) {
                        if (logger.isDebugEnabled())
                            logger.debug("Collection " + coll + " does not exist - ensuring indices");
                        morphium.ensureIndicesFor(type);
                    }

                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    WriteResult result = null;
                    if (wc != null) {
                        result = morphium.getDatabase().getCollection(coll).save(marshall, wc);
                    } else {

                        result = morphium.getDatabase().getCollection(coll).save(marshall);
                    }
                    if (!result.getLastError().ok()) {
                        logger.error("Writing failed: " + result.getLastError().getErrorMessage());
                    }

                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), marshall, dur, true, WriteAccessType.SINGLE_INSERT);
//                    if (logger.isDebugEnabled()) {
//                        String n = "";
//                        if (isNew) {
//                            n = "NEW ";
//                        }
//                        logger.debug(n + "stored " + type.getSimpleName() + " after " + dur + " ms length:" + marshall.toString().length());
//                    }
                    if (isNew) {
                        List<String> flds = annotationHelper.getFields(o.getClass(), Id.class);
                        if (flds == null) {
                            throw new RuntimeException("Object does not have an ID field!");
                        }
                        try {
                            //Setting new ID (if object was new created) to Entity
                            annotationHelper.getField(o.getClass(), flds.get(0)).set(o, marshall.get("_id"));
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    Cache ch = annotationHelper.getAnnotationFromHierarchy(o.getClass(), Cache.class);
                    if (ch != null) {
                        if (ch.clearOnWrite()) {
                            morphium.clearCachefor(o.getClass());
                        }
                    }

                    morphium.firePostStoreEvent(o, isNew);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, null, obj);
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        if (e.getClass().getName().equals("javax.validation.ConstraintViolationException")) {
                            //using reflection to get fields etc... in order to remove strong dependency
                            try {
                                Method m = e.getClass().getMethod("getConstraintViolations");

                                Set violations = (Set) m.invoke(e);
                                for (Object v : violations) {
                                    m = v.getClass().getMethod("getMessage");
                                    String msg = (String) m.invoke(v);
                                    m = v.getClass().getMethod("getRootBean");
                                    Object bean = m.invoke(v);
                                    String s = morphium.toJsonString(bean);
                                    String type = bean.getClass().getName();
                                    m = v.getClass().getMethod("getInvalidValue");
                                    Object invalidValue = m.invoke(v);
                                    m = v.getClass().getMethod("getPropertyPath");
                                    Iterable<?> pth = (Iterable) m.invoke(v);
                                    String path = "";
                                    for (Object p : pth) {
                                        m = p.getClass().getMethod("getName");
                                        String name = (String) m.invoke(p);
                                        path = path + "." + name;
                                    }
                                    logger.error("Validation of " + type + " failed: " + msg + " - Invalid Value: " + invalidValue + " for path: " + path + "\n Tried to store: " + s);
                                }
                            } catch (Exception e1) {
                                logger.fatal("Could not get more information about validation error ", e1);
                            }
                        }
                        throw (RuntimeException) e;
                    }
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, e.getMessage(), e, obj);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void store(final List<T> lst, String collectionName, final AsyncOperationCallback<T> callback) {
        if (lst == null || lst.size() == 0) return;
        if (!morphium.getDatabase().collectionExists(collectionName)) {
            logger.warn("collection does not exist while storing list -  taking first element of list to ensure indices");
            morphium.ensureIndicesFor((Class<T>) lst.get(0).getClass(), collectionName, callback);
        }
        ArrayList<DBObject> dbLst = new ArrayList<DBObject>();
        DBCollection collection = morphium.getDatabase().getCollection(collectionName);
        WriteConcern wc = morphium.getWriteConcernForClass(lst.get(0).getClass());
        HashMap<Object, Boolean> isNew = new HashMap<Object, Boolean>();
        for (Object record : lst) {
            DBObject marshall = morphium.getMapper().marshall(record);
            isNew.put(record, annotationHelper.getId(record) == null);
            if (isNew.get(record)) {
                dbLst.add(marshall);
            } else {
                //single update
                long start = System.currentTimeMillis();
                WriteResult result = null;
                if (wc == null) {
                    result = collection.save(marshall);
                } else {
                    result = collection.save(marshall, wc);
                }
                if (!result.getLastError().ok()) {
                    logger.error("Writing failed: " + result.getLastError().getErrorMessage());
                }
                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(lst.get(0).getClass(), marshall, dur, false, WriteAccessType.SINGLE_INSERT);
                morphium.firePostStoreEvent(record, isNew.get(record));
            }

        }
        long start = System.currentTimeMillis();

        if (wc == null) {
            collection.insert(dbLst);
        } else {
            collection.insert(dbLst, wc);
        }
        long dur = System.currentTimeMillis() - start;
        //bulk insert
        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), dbLst, dur, true, WriteAccessType.BULK_INSERT);
        for (Object record : lst) {
            if (isNew.get(record)) {
                morphium.firePostStoreEvent(record, isNew.get(record));
            }
        }
    }

    @Override
    public void flush() {
        //nothing to do
    }

    @Override
    public <T> void store(final List<T> lst, final AsyncOperationCallback<T> callback) {
        if (!lst.isEmpty()) {
            Runnable r = new Runnable() {
                public void run() {
                    HashMap<Class, List<Object>> sorted = new HashMap<Class, List<Object>>();
                    HashMap<Object, Boolean> isNew = new HashMap<Object, Boolean>();
                    for (Object o : lst) {
                        Class type = annotationHelper.getRealClass(o.getClass());
                        if (!annotationHelper.isAnnotationPresentInHierarchy(type, Entity.class)) {
                            logger.error("Not an entity! Storing not possible! Even not in list!");
                            continue;
                        }
                        morphium.inc(StatisticKeys.WRITES);
                        if (annotationHelper.isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
                            //not part of list, acutally...
                            if ((o instanceof PartiallyUpdateable)) {
                                morphium.updateUsingFields(o, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
                                ((PartiallyUpdateable) o).clearAlteredFields();
                                continue;
                            }
                        }
                        o = annotationHelper.getRealObject(o);
                        if (o == null) {
                            logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                            return;
                        }

                        if (sorted.get(o.getClass()) == null) {
                            sorted.put(o.getClass(), new ArrayList<Object>());
                        }
                        sorted.get(o.getClass()).add(o);
                        if (morphium.getId(o) == null) {
                            isNew.put(o, true);
                        } else {
                            isNew.put(o, false);
                        }
                        morphium.firePreStoreEvent(o, isNew.get(o));
                    }
                    long allStart = System.currentTimeMillis();
                    try {
                        for (Map.Entry<Class, List<Object>> es : sorted.entrySet()) {
                            Class c = es.getKey();
                            ArrayList<DBObject> dbLst = new ArrayList<DBObject>();
                            //bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll = morphium.getMapper().getCollectionName(c);
                            DBCollection collection = morphium.getDatabase().getCollection(coll);
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                if (logger.isDebugEnabled())
                                    logger.debug("Collection does not exist - ensuring indices");
                                morphium.ensureIndicesFor(c);
                            }
                            for (Object record : es.getValue()) {
                                DBObject marshall = morphium.getMapper().marshall(record);
                                if (isNew.get(record)) {
                                    dbLst.add(marshall);
                                } else {
                                    //single update
                                    long start = System.currentTimeMillis();
                                    WriteResult result = null;
                                    if (wc == null) {
                                        result = collection.save(marshall);
                                    } else {
                                        result = collection.save(marshall, wc);
                                    }
                                    if (!result.getLastError().ok()) {
                                        logger.error("Writing failed: " + result.getLastError().getErrorMessage());
                                    }
                                    long dur = System.currentTimeMillis() - start;
                                    morphium.fireProfilingWriteEvent(c, marshall, dur, false, WriteAccessType.SINGLE_INSERT);
                                    morphium.firePostStoreEvent(record, isNew.get(record));
                                }

                            }
                            long start = System.currentTimeMillis();

                            if (wc == null) {
                                collection.insert(dbLst);
                            } else {
                                collection.insert(dbLst, wc);
                            }
                            long dur = System.currentTimeMillis() - start;
                            //bulk insert
                            morphium.fireProfilingWriteEvent(c, dbLst, dur, true, WriteAccessType.BULK_INSERT);
                            for (Object record : es.getValue()) {
                                if (isNew.get(record)) {
                                    morphium.firePostStoreEvent(record, isNew.get(record));
                                }
                            }
                        }
                        if (callback != null)
                            callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, null, null, lst);
                    } catch (Exception e) {
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        }
                        if (callback == null) throw new RuntimeException(e);
                        callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - allStart, e.getMessage(), e, null, lst);
                    }
                }
            };
            submitAndBlockIfNecessary(callback, r);
        }
    }


    @Override
    public <T> void set(final T toSet, final String collection, final String field, final Object v, final boolean insertIfNotExist, final boolean multiple, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Class cls = toSet.getClass();
                Object value = v;
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                value = marshallIfNecessary(value);
                String coll = collection;
                if (coll == null) {
                    morphium.getMapper().getCollectionName(cls);
                }
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(toSet));
                Field f = annotationHelper.getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = annotationHelper.getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$set", new BasicDBObject(fieldName, value));

                WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());
                long start = System.currentTimeMillis();

                try {
                    if (wc == null) {
                        morphium.getDatabase().getCollection(coll).update(query, update, insertIfNotExist, multiple);
                    } else {
                        morphium.getDatabase().getCollection(coll).update(query, update, insertIfNotExist, multiple, wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    try {
                        f.set(toSet, value);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, null, System.currentTimeMillis() - start, null, toSet, field, v);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, null, System.currentTimeMillis() - start, e.getMessage(), e, toSet, field, v);
                }
                morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }


    public <T> void submitAndBlockIfNecessary(AsyncOperationCallback<T> callback, Runnable r) {
        if (callback == null) {
            r.run();
        } else {
            while (writeBufferCount() >= morphium.getConfig().getMaxConnections() * morphium.getConfig().getBlockingThreadsMultiplier() * 0.75 - 1) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Have to wait for queue to be more empty - active threads now: " + writeBufferCount());
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    //TODO: Implement Handling
                    throw new RuntimeException(e);
                }
            }
            executor.submit(r);
        }
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, final AsyncOperationCallback<T> callback, final String... fields) {
        if (ent == null) return;
        Runnable r = new Runnable() {
            @Override
            public void run() {
                ObjectId id = annotationHelper.getId(ent);
                if (id == null) {
                    //new object - update not working
                    logger.warn("trying to partially update new object - storing it in full!");
                    store(ent, collection, callback);
                    return;
                }
                morphium.firePreStoreEvent(ent, false);
                morphium.inc(StatisticKeys.WRITES);
                DBObject find = new BasicDBObject();

                find.put("_id", id);
                DBObject update = new BasicDBObject();
                for (String f : fields) {
                    try {
                        Object value = annotationHelper.getValue(ent, f);
                        if (annotationHelper.isAnnotationPresentInHierarchy(value.getClass(), Entity.class)) {
                            value = morphium.getMapper().marshall(value);
                        }
                        update.put(f, value);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                Class<?> type = annotationHelper.getRealClass(ent.getClass());

                LastChange t = annotationHelper.getAnnotationFromHierarchy(type, LastChange.class); //(StoreLastChange) type.getAnnotation(StoreLastChange.class);
                if (t != null) {
                    List<String> lst = annotationHelper.getFields(ent.getClass(), LastChange.class);

                    long now = System.currentTimeMillis();
                    for (String ctf : lst) {
                        Field f = annotationHelper.getField(type, ctf);
                        if (f != null) {
                            try {
                                f.set(ent, now);
                            } catch (IllegalAccessException e) {
                                logger.error("Could not set modification time", e);

                            }
                        }
                        update.put(ctf, now);
                    }
                }


                update = new BasicDBObject("$set", update);
                WriteConcern wc = morphium.getWriteConcernForClass(type);
                long start = System.currentTimeMillis();
                try {
                    String collectionName = morphium.getMapper().getCollectionName(ent.getClass());
                    if (collectionName == null) {
                        collectionName = collection;
                    }
                    if (wc != null) {
                        morphium.getDatabase().getCollection(collectionName).update(find, update, false, false, wc);
                    } else {
                        morphium.getDatabase().getCollection(collectionName).update(find, update, false, false);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(ent.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(annotationHelper.getRealClass(ent.getClass()));
                    morphium.firePostStoreEvent(ent, false);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.UPDATE, null, System.currentTimeMillis() - start, null, ent, fields);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.UPDATE, null, System.currentTimeMillis() - start, e.getMessage(), e, ent, fields);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }


    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public <T> void delete(final List<T> lst, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                HashMap<Class<T>, List<Query<T>>> sortedMap = new HashMap<Class<T>, List<Query<T>>>();

                for (T o : lst) {
                    if (sortedMap.get(o.getClass()) == null) {
                        List<Query<T>> queries = new ArrayList<Query<T>>();
                        sortedMap.put((Class<T>) o.getClass(), queries);
                    }
                    Query<T> q = (Query<T>) morphium.createQueryFor(o.getClass());
                    q.f(annotationHelper.getIdFieldName(o)).eq(annotationHelper.getId(o));
                    sortedMap.get(o.getClass()).add(q);
                }
                long start = System.currentTimeMillis();
                try {
                    for (Class<T> cls : sortedMap.keySet()) {
                        Query<T> orQuery = morphium.createQueryFor(cls);
                        orQuery = orQuery.or(sortedMap.get(cls));
                        delete(orQuery, (AsyncOperationCallback<T>) null); //sync call
                    }
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, null, null, lst);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, e.getMessage(), e, null, lst);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * deletes all objects matching the given query
     *
     * @param q - query
     */
    @Override
    public <T> void delete(final Query<T> q, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                morphium.firePreRemoveEvent(q);
                WriteConcern wc = morphium.getWriteConcernForClass(q.getType());
                long start = System.currentTimeMillis();
                try {
                    if (wc == null) {
                        morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject());
                    } else {
                        morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject(), wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(q.getType(), q.toQueryObject(), dur, false, WriteAccessType.BULK_DELETE);
                    morphium.getCache().clearCacheIfNecessary(q.getType());
                    morphium.firePostRemoveEvent(q);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, q, System.currentTimeMillis() - start, null, null);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.REMOVE, q, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);


    }

    @Override
    public <T> void delete(final T o, final String collection, final AsyncOperationCallback<T> callback) {

        Runnable r = new Runnable() {
            @Override
            public void run() {
                ObjectId id = annotationHelper.getId(o);
                morphium.firePreRemoveEvent(o);
                BasicDBObject db = new BasicDBObject();
                db.append("_id", id);
                WriteConcern wc = morphium.getWriteConcernForClass(o.getClass());

                long start = System.currentTimeMillis();
                try {
                    String collectionName = collection;
                    if (collectionName == null) {
                        morphium.getMapper().getCollectionName(o.getClass());
                    }
                    if (wc == null) {
                        morphium.getDatabase().getCollection(collectionName).remove(db);
                    } else {
                        morphium.getDatabase().getCollection(collectionName).remove(db, wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), o, dur, false, WriteAccessType.SINGLE_DELETE);
                    morphium.clearCachefor(o.getClass());
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.firePostRemoveEvent(o);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, null, o);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, e.getMessage(), e, o);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);

    }

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc:  object to set the value in (or better - the corresponding entry in mongo)
     * @param field:  the field to change
     * @param amount: the value to set
     */
    @Override
    public <T> void inc(final T toInc, final String collection, final String field, final int amount, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Class cls = toInc.getClass();
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(toInc));
                Field f = annotationHelper.getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = annotationHelper.getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
                WriteConcern wc = morphium.getWriteConcernForClass(toInc.getClass());
                long start = System.currentTimeMillis();
                try {
                    if (wc == null) {
                        morphium.getDatabase().getCollection(coll).update(query, update);
                    } else {
                        morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                    }

                    morphium.getCache().clearCacheIfNecessary(cls);

                    if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                        try {
                            f.set(toInc, ((Integer) f.get(toInc)) + amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                        try {
                            f.set(toInc, ((Double) f.get(toInc)) + amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                        try {
                            f.set(toInc, ((Float) f.get(toInc)) + (float) amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                        try {
                            f.set(toInc, ((Long) f.get(toInc)) + (long) amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        logger.error("Could not set increased value - unsupported type " + cls.getName());
                    }
                    morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    morphium.fireProfilingWriteEvent(toInc.getClass(), toInc, System.currentTimeMillis() - start, false, WriteAccessType.SINGLE_UPDATE);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.INC, null, System.currentTimeMillis() - start, null, toInc, field, amount);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.INC, null, System.currentTimeMillis() - start, e.getMessage(), e, toInc, field, amount);
                }
            }
        };


        submitAndBlockIfNecessary(callback, r);


    }

    @Override
    public <T> void inc(final Query<T> query, final String field, final int amount, final boolean insertIfNotExist, final boolean multiple, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Class<?> cls = query.getType();
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = morphium.getMapper().getCollectionName(cls);
                String fieldName = annotationHelper.getFieldName(cls, field);
                BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }
                if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                    morphium.ensureIndicesFor(cls);
                }
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    if (wc == null) {
                        morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                    } else {
                        morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.INC, query, System.currentTimeMillis() - start, null, null, field, amount);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.INC, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, amount);

                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }


    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query            - query to specify which objects should be set
     * @param values           - map fieldName->Value, which values are to be set!
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple         - update several documents, if false, only first hit will be updated
     */
    @Override
    public <T> void set(final Query<T> query, final Map<String, Object> values, final boolean insertIfNotExist, final boolean multiple, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = morphium.getMapper().getCollectionName(cls);
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                BasicDBObject toSet = new BasicDBObject();
                for (Map.Entry<String, Object> ef : values.entrySet()) {
                    String fieldName = annotationHelper.getFieldName(cls, ef.getKey());
                    toSet.put(fieldName, marshallIfNecessary(ef.getValue()));
                }
                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                    morphium.ensureIndicesFor(cls);
                }

                BasicDBObject update = new BasicDBObject("$set", toSet);
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    if (wc == null) {
                        morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                    } else {
                        morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, values, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, values, insertIfNotExist, multiple);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    @Override
    public <T> void unset(final T toSet, final String collection, final String field, final AsyncOperationCallback<T> callback) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");
        if (annotationHelper.getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet, collection, callback);
        }

        Runnable r = new Runnable() {
            @Override
            public void run() {
                Class cls = toSet.getClass();
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(toSet));
                Field f = annotationHelper.getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = annotationHelper.getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$unset", new BasicDBObject(fieldName, 1));
                WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());
                if (!morphium.getDatabase().collectionExists(coll)) {
                    morphium.ensureIndicesFor(cls);
                }
                long start = System.currentTimeMillis();

                try {
                    if (wc == null) {
                        morphium.getDatabase().getCollection(coll).update(query, update);
                    } else {
                        morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(toSet.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);

                    try {
                        f.set(toSet, null);
                    } catch (IllegalAccessException e) {
                        //May happen, if null is not allowed for example
                    }
                    morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, null, toSet, field);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, e.getMessage(), e, toSet, field);
                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }


    @Override
    public <T> void pushPull(final boolean push, final Query<T> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Class<?> cls = query.getType();
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

                String coll = morphium.getMapper().getCollectionName(cls);

                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }
                Object v = marshallIfNecessary(value);

                String fieldName = annotationHelper.getFieldName(cls, field);
                BasicDBObject set = new BasicDBObject(fieldName, v);
                BasicDBObject update = new BasicDBObject(push ? "$push" : "$pull", set);
                long start = System.currentTimeMillis();

                try {
                    pushIt(push, insertIfNotExist, multiple, cls, coll, qobj, update);
                    morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.PUSH, query, System.currentTimeMillis() - start, null, null, field, value, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.PUSH, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, value, insertIfNotExist, multiple);
                }
            }
        };

        submitAndBlockIfNecessary(callback, r);
    }

    private Object marshallIfNecessary(Object value) {
        if (value != null) {
            if (annotationHelper.isAnnotationPresentInHierarchy(value.getClass(), Entity.class)
                    || annotationHelper.isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
                //need to marshall...
                DBObject marshall = morphium.getMapper().marshall(value);
                marshall.put("class_name", annotationHelper.getRealClass(value.getClass()).getName());
                value = marshall;
            } else if (List.class.isAssignableFrom(value.getClass())) {
                List lst = new ArrayList();
                for (Object o : (List) value) {
                    if (annotationHelper.isAnnotationPresentInHierarchy(o.getClass(), Embedded.class) ||
                            annotationHelper.isAnnotationPresentInHierarchy(o.getClass(), Entity.class)
                            ) {
                        DBObject marshall = morphium.getMapper().marshall(o);
                        marshall.put("class_name", annotationHelper.getRealClass(o.getClass()).getName());

                        lst.add(marshall);
                    } else {
                        lst.add(o);
                    }
                }
                value = lst;
            } else if (Map.class.isAssignableFrom(value.getClass())) {
                for (Object e : ((Map) value).entrySet()) {
                    Map.Entry en = (Map.Entry) e;
                    if (!String.class.isAssignableFrom(((Map.Entry) e).getKey().getClass())) {
                        throw new IllegalArgumentException("Can't push maps with Key not of type String!");
                    }
                    if (annotationHelper.isAnnotationPresentInHierarchy(en.getValue().getClass(), Entity.class) ||
                            annotationHelper.isAnnotationPresentInHierarchy(en.getValue().getClass(), Embedded.class)
                            ) {
                        DBObject marshall = morphium.getMapper().marshall(en.getValue());
                        marshall.put("class_name", annotationHelper.getRealClass(en.getValue().getClass()).getName());
                        ((Map) value).put(en.getKey(), marshall);
                    }
                }
            }
        }
        return value;
    }


    private void pushIt(boolean push, boolean insertIfNotExist, boolean multiple, Class<?> cls, String coll, DBObject qobj, BasicDBObject update) {
        if (!morphium.getDatabase().collectionExists(coll) && insertIfNotExist) {
            morphium.ensureIndicesFor(cls);
        }
        WriteConcern wc = morphium.getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        if (wc == null) {
            morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
        } else {
            morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
    }

    @Override
    public <T> void pushPullAll(final boolean push, final Query<T> query, final String f, final List<?> v, final boolean insertIfNotExist, final boolean multiple, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                List<?> value = v;
                String field = f;
                Class<?> cls = query.getType();
                String coll = morphium.getMapper().getCollectionName(cls);
                morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
                long start = System.currentTimeMillis();
                List lst = new ArrayList();
                for (Object o : value) {
                    lst.add(marshallIfNecessary(o));
                }
                value = lst;
                try {
                    DBObject qobj = query.toQueryObject();
                    if (insertIfNotExist) {
                        qobj = morphium.simplifyQueryObject(qobj);
                    }
                    field = annotationHelper.getFieldName(cls, field);
                    BasicDBObject set = new BasicDBObject(field, value);
                    BasicDBObject update = new BasicDBObject(push ? "$pushAll" : "$pullAll", set);
                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    if (wc == null) {
                        morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                    } else {
                        morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    if (callback != null)
                        callback.onOperationSucceeded(push ? AsyncOperationType.PUSH : AsyncOperationType.PULL, query, System.currentTimeMillis() - start, null, null, field, value, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(push ? AsyncOperationType.PUSH : AsyncOperationType.PULL, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, value, insertIfNotExist, multiple);
                }
            }

        };
        submitAndBlockIfNecessary(callback, r);
    }


    @Override
    public <T> void dropCollection(final Class<T> cls, final String collection, AsyncOperationCallback<T> callback) {
        if (annotationHelper.isAnnotationPresentInHierarchy(cls, Entity.class)) {
            Runnable r = new Runnable() {
                public void run() {
                    morphium.firePreDropEvent(cls);
                    long start = System.currentTimeMillis();
                    String co = collection;
                    if (co == null) {
                        co = morphium.getMapper().getCollectionName(cls);
                    }
                    DBCollection coll = morphium.getDatabase().getCollection(co);
//            coll.setReadPreference(com.mongodb.ReadPreference.PRIMARY);
                    coll.drop();
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, null, dur, false, WriteAccessType.DROP);
                    morphium.firePostDropEvent(cls);
                }
            };
            submitAndBlockIfNecessary(callback, r);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    @Override
    public <T> void ensureIndex(final Class<T> cls, final String collection, final Map<String, Object> index, final Map<String, Object> options, AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            public void run() {
                List<String> fields = annotationHelper.getFields(cls);

                Map<String, Object> idx = new LinkedHashMap<String, Object>();
                for (Map.Entry<String, Object> es : index.entrySet()) {
                    String k = es.getKey();
                    if (!fields.contains(k) && !fields.contains(annotationHelper.convertCamelCase(k))) {
                        throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
                    }
                    String fn = annotationHelper.getFieldName(cls, k);
                    idx.put(fn, es.getValue());
                }
                long start = System.currentTimeMillis();
                BasicDBObject keys = new BasicDBObject(idx);
                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                if (options == null) {
                    morphium.getDatabase().getCollection(coll).ensureIndex(keys);
                } else {
                    BasicDBObject opts = new BasicDBObject(options);
                    morphium.getDatabase().getCollection(coll).ensureIndex(keys, opts);
                }
                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(cls, keys, dur, false, WriteAccessType.ENSURE_INDEX);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public int writeBufferCount() {
        return executor.getActiveCount();
    }
}
