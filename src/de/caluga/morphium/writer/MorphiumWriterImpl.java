package de.caluga.morphium.writer;

import com.mongodb.*;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
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
public class MorphiumWriterImpl implements MorphiumWriter, ShutdownListener {
    private static Logger logger = new Logger(MorphiumWriterImpl.class);
    private Morphium morphium;
    private int maximumRetries = 10;
    private int pause = 250;
    private ThreadPoolExecutor executor = null;

    @Override
    public void setMaximumQueingTries(int n) {
        maximumRetries = n;
    }

    @Override
    public void setPauseBetweenTries(int p) {
        pause = p;
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        if (m != null) {
            executor = new ThreadPoolExecutor(m.getConfig().getMaxConnections() / 2, (int) (m.getConfig().getMaxConnections() * m.getConfig().getBlockingThreadsMultiplier() * 0.9),
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
            m.addShutdownListener(this);
        }
    }

    /**
     * @param obj - object to store
     */
    @Override
    public <T> void store(final T obj, final String collection, AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            store((List) obj, callback);
            return;
        }
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            public void run() {
                long start = System.currentTimeMillis();

                try {
                    T o = obj;
                    Class type = morphium.getARHelper().getRealClass(o.getClass());
                    if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }
                    morphium.inc(StatisticKeys.WRITES);
                    Object id = morphium.getARHelper().getId(o);
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
                        if ((o instanceof PartiallyUpdateable)) {
                            updateUsingFields(o, collection, callback, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
                            ((PartiallyUpdateable) o).clearAlteredFields();

                            return;
                        }
                    }
                    o = morphium.getARHelper().getRealObject(o);
                    if (o == null) {
                        logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                        return;
                    }
                    boolean isNew = id == null;
                    Object reread = null;
                    if (morphium.isAutoValuesEnabledForThread()) {
                        CreationTime creationTime = morphium.getARHelper().getAnnotationFromHierarchy(type, CreationTime.class);
                        if (id != null && (morphium.getConfig().isCheckForNew() || (creationTime != null && creationTime.checkForNew()))
                                && !morphium.getARHelper().getIdField(o).getType().equals(ObjectId.class)) {
                            //check if it exists
                            reread = morphium.findById(o.getClass(), id);
                            isNew = reread == null;
                        }
                        isNew = setAutoValues(o, type, id, isNew, reread);

                    }
                    morphium.firePreStoreEvent(o, isNew);

                    DBObject marshall = morphium.getMapper().marshall(o);

                    String coll = collection;
                    if (coll == null) {
                        coll = morphium.getMapper().getCollectionName(type);
                    }
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                if (logger.isDebugEnabled())
                                    logger.debug("Collection " + coll + " does not exist - ensuring indices");
                                createCappedColl(o.getClass());
                                morphium.ensureIndicesFor(type, coll, callback);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }

                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    WriteResult result = null;
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (wc != null) {
                                result = morphium.getDatabase().getCollection(coll).save(marshall, wc);
                            } else {

                                result = morphium.getDatabase().getCollection(coll).save(marshall);
                            }
//                            if (!result.getLastError().ok()) {
//                                logger.error("Writing failed: " + result.getLastError().getErrorMessage());
//                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
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
                        List<String> flds = morphium.getARHelper().getFields(o.getClass(), Id.class);
                        if (flds == null) {
                            throw new RuntimeException("Object does not have an ID field!");
                        }
                        try {
                            //Setting new ID (if object was new created) to Entity

                            Field fld = morphium.getARHelper().getField(o.getClass(), flds.get(0));
                            if (fld.getType().equals(marshall.get("_id").getClass())) {
                                fld.set(o, marshall.get("_id"));
                            } else {
                                logger.warn("got default generated key, but ID-Field is not of type ObjectID... trying string conversion");
                                if (fld.getType().equals(String.class)) {
                                    fld.set(o, marshall.get("_id").toString());
                                } else {
                                    throw new IllegalArgumentException("cannot convert ID for given object - id type is: " + fld.getType().getName() + "! Please set ID before write");
                                }
                            }
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    morphium.getCache().clearCacheIfNecessary(o.getClass());
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
                    }
                    if (callback == null) {
                        if (e instanceof RuntimeException) {
                            throw ((RuntimeException) e);
                        }
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, e.getMessage(), e, obj);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    private <T> boolean setAutoValues(T o, Class type, Object id, boolean aNew, Object reread) throws IllegalAccessException {
        if (!morphium.isAutoValuesEnabledForThread()) return aNew;
        //new object - need to store creation time
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(type, CreationTime.class)) {
            CreationTime ct = morphium.getARHelper().getAnnotationFromHierarchy(o.getClass(), CreationTime.class);
            boolean checkForNew = ct.checkForNew() || morphium.getConfig().isCheckForNew();
            List<String> lst = morphium.getARHelper().getFields(type, CreationTime.class);
            for (String fld : lst) {
                Field field = morphium.getARHelper().getField(o.getClass(), fld);
                if (id != null) {
                    if (checkForNew && reread == null) {
                        reread = morphium.findById(o.getClass(), id);
                        aNew = reread == null;
                    } else {
                        if (reread == null) {
                            aNew = (id instanceof ObjectId && id == null); //if id null, is new. if id!=null probably not, if type is objectId
                        } else {
                            Object value = field.get(reread);
                            field.set(o, value);
                            aNew = false;
                        }
                    }
                } else {
                    aNew = true;
                }
            }
            if (aNew) {
                if (lst == null || lst.size() == 0) {
                    logger.error("Unable to store creation time as @CreationTime for field is missing");
                } else {
                    long now = System.currentTimeMillis();
                    for (String ctf : lst) {
                        Object val = null;

                        Field f = morphium.getARHelper().getField(type, ctf);
                        if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                            val = now;
                        } else if (f.getType().equals(Date.class)) {
                            val = new Date(now);
                        } else if (f.getType().equals(String.class)) {
                            CreationTime ctField = f.getAnnotation(CreationTime.class);
                            SimpleDateFormat df = new SimpleDateFormat(ctField.dateFormat());
                            val = df.format(now);
                        }

                        if (f != null) {
                            try {
                                f.set(o, val);
                            } catch (IllegalAccessException e) {
                                logger.error("Could not set creation time", e);

                            }
                        }

                    }

                }

            }
        }


        if (morphium.getARHelper().isAnnotationPresentInHierarchy(type, LastChange.class)) {
            List<String> lst = morphium.getARHelper().getFields(type, LastChange.class);
            if (lst != null && lst.size() > 0) {
                long now = System.currentTimeMillis();
                for (String ctf : lst) {
                    Object val = null;

                    Field f = morphium.getARHelper().getField(type, ctf);
                    if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                        val = now;
                    } else if (f.getType().equals(Date.class)) {
                        val = new Date(now);
                    } else if (f.getType().equals(String.class)) {
                        LastChange ctField = f.getAnnotation(LastChange.class);
                        SimpleDateFormat df = new SimpleDateFormat(ctField.dateFormat());
                        val = df.format(now);
                    }

                    if (f != null) {
                        try {
                            f.set(o, val);
                        } catch (IllegalAccessException e) {
                            logger.error("Could not set modification time", e);

                        }
                    }
                }
            } else {
                logger.warn("Could not store last change - @LastChange missing!");
            }

        }
        return aNew;
    }

    @Override
    public <T> void store(final List<T> lst, String collectionName, AsyncOperationCallback<T> callback) {
        if (lst == null || lst.size() == 0) return;
        ArrayList<DBObject> dbLst = new ArrayList<>();
        DBCollection collection = morphium.getDatabase().getCollection(collectionName);
        WriteConcern wc = morphium.getWriteConcernForClass(lst.get(0).getClass());

        BulkWriteOperation bulkWriteOperation = collection.initializeUnorderedBulkOperation();
        HashMap<Object, Boolean> isNew = new HashMap<>();
        if (!morphium.getDatabase().collectionExists(collectionName)) {
            logger.warn("collection does not exist while storing list -  taking first element of list to ensure indices");
            morphium.ensureIndicesFor((Class<T>) lst.get(0).getClass(), collectionName, callback);
        }
        long start = System.currentTimeMillis();
        int cnt = 0;
        List<Class<?>> types = new ArrayList<>();
        for (Object record : lst) {
            DBObject marshall = morphium.getMapper().marshall(record);
            Object id = morphium.getARHelper().getId(record);
            boolean isn = id == null;
            Object reread = null;
            if (morphium.isAutoValuesEnabledForThread()) {
                CreationTime creationTime = morphium.getARHelper().getAnnotationFromHierarchy(record.getClass(), CreationTime.class);
                if (!isn && (morphium.getConfig().isCheckForNew() || (creationTime != null && creationTime.checkForNew()))
                        && !morphium.getARHelper().getIdField(record).getType().equals(ObjectId.class)) {
                    //check if it exists
                    reread = morphium.findById(record.getClass(), id);
                    isn = reread == null;
                }
                try {
                    isn = setAutoValues(record, record.getClass(), id, isn, reread);
                } catch (IllegalAccessException e) {
                    logger.error(e);
                }
            }
            isNew.put(record, isn);

            if (!types.contains(record.getClass())) types.add(record.getClass());
            if (isNew.get(record)) {
                dbLst.add(marshall);
            } else {
                //single update
                WriteResult result = null;
                cnt++;
                BulkUpdateRequestBuilder up = bulkWriteOperation.find(new BasicDBObject("_id", morphium.getARHelper().getId(record))).upsert();
                up.updateOne(new BasicDBObject("$set", marshall));
            }
        }
        morphium.firePreStoreEvent(isNew);
        if (cnt > 0) {
            for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                try {
                    //storing updates
                    if (wc == null) {
                        bulkWriteOperation.execute();
                    } else {
                        bulkWriteOperation.execute(wc);
                    }
                } catch (Exception e) {
                    morphium.handleNetworkError(i, e);
                }
            }
            for (Class<?> c : types) {
                morphium.getCache().clearCacheIfNecessary(c);
            }
        }

        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), lst, dur, false, WriteAccessType.BULK_UPDATE);

        start = System.currentTimeMillis();

        if (wc == null) {
            collection.insert(dbLst);
        } else {
            collection.insert(dbLst, wc);
        }
        dur = System.currentTimeMillis() - start;
        //bulk insert
        morphium.fireProfilingWriteEvent(lst.get(0).getClass(), dbLst, dur, true, WriteAccessType.BULK_INSERT);
        morphium.firePostStore(isNew);
    }

    @Override
    public void flush() {
        //nothing to do
    }

    @Override
    public <T> void store(final List<T> lst, AsyncOperationCallback<T> callback) {
        if (!lst.isEmpty()) {
            WriterTask r = new WriterTask() {
                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                public void run() {
                    HashMap<Class, List<Object>> sorted = new HashMap<>();
                    HashMap<Object, Boolean> isNew = new HashMap<>();
                    int cnt = 0;
                    for (Object o : lst) {
                        Class type = morphium.getARHelper().getRealClass(o.getClass());
                        if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                            logger.error("Not an entity! Storing not possible! Even not in list!");
                            continue;
                        }
                        morphium.inc(StatisticKeys.WRITES);
                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
                            //not part of list, acutally...
                            if ((o instanceof PartiallyUpdateable)) {
                                //todo: use batch write
                                morphium.updateUsingFields(o, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
                                ((PartiallyUpdateable) o).clearAlteredFields();
                                continue;
                            }
                        }
                        o = morphium.getARHelper().getRealObject(o);
                        if (o == null) {
                            logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                            return;
                        }


                        if (sorted.get(o.getClass()) == null) {
                            sorted.put(o.getClass(), new ArrayList<>());
                        }
                        sorted.get(o.getClass()).add(o);
                        boolean isn = morphium.getId(o) == null;
                        Object reread = null;
                        if (morphium.getARHelper().getAnnotationFromHierarchy(o.getClass(), CreationTime.class) != null) {
                            try {
                                isn = setAutoValues(o, o.getClass(), morphium.getId(o), isn, reread);
                            } catch (IllegalAccessException e) {
                                logger.error(e);
                            }
                        }
                        if (isn) {
                            if (!morphium.getARHelper().getIdField(o).getType().equals(ObjectId.class) && morphium.getId(o) == null) {
                                throw new IllegalArgumentException("Cannot automatically set non -ObjectId IDs");
                            }
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
                            ArrayList<DBObject> dbLst = new ArrayList<>();
                            //bulk insert... check if something already exists
                            WriteConcern wc = morphium.getWriteConcernForClass(c);
                            String coll = morphium.getMapper().getCollectionName(c);
                            DBCollection collection = null;
                            for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                                try {
                                    collection = morphium.getDatabase().getCollection(coll);
                                    if (!morphium.getDatabase().collectionExists(coll)) {
                                        createCappedColl(c);
                                        morphium.ensureIndicesFor(c, coll, callback);
                                    }
                                    break;
                                } catch (Throwable t) {
                                    morphium.handleNetworkError(i, t);
                                }
                            }

                            BulkWriteOperation bulkWriteOperation = collection.initializeUnorderedBulkOperation();
                            long start = System.currentTimeMillis();

                            HashMap<Integer, Object> mapMarshalledNewObjects = new HashMap<>();
                            for (Object record : es.getValue()) {
                                DBObject marshall = morphium.getMapper().marshall(record);
                                if (isNew.get(record)) {
                                    dbLst.add(marshall);
                                    mapMarshalledNewObjects.put(dbLst.size() - 1, record);
                                } else {
                                    //bulk update
                                    BulkWriteRequestBuilder findId = bulkWriteOperation.find(new BasicDBObject("_id", morphium.getARHelper().getId(record)));
                                    findId.upsert().updateOne(new BasicDBObject("$set", marshall));
                                    cnt++;
                                    if (cnt >= morphium.getMaxWriteBatchSize()) {
                                        executeWriteBatch(es.getValue(), c, wc, bulkWriteOperation, start);
                                        cnt = 0;
                                        bulkWriteOperation = collection.initializeUnorderedBulkOperation();
                                    }
                                }
                            }

                            if (cnt > 0)
                                executeWriteBatch(es.getValue(), c, wc, bulkWriteOperation, start);
                            start = System.currentTimeMillis();
                            if (dbLst.size() > morphium.getMaxWriteBatchSize()) {
                                int l = morphium.getMaxWriteBatchSize();
                                for (int idx = 0; idx < dbLst.size(); idx += l) {
                                    int end = idx + l;
                                    if (end > dbLst.size()) {
                                        end = dbLst.size();
                                    }
                                    List<DBObject> lst = dbLst.subList(idx, end);
                                    doStoreList(lst, wc, collection);
                                    for (DBObject o : lst) {
                                        Object entity = mapMarshalledNewObjects.get(dbLst.indexOf(o));
                                        if (entity == null) {
                                            logger.error("cannot update mongo_id...");
                                        } else {
                                            try {
                                                morphium.getARHelper().getIdField(entity).set(entity, o.get("_id"));
                                            } catch (Exception e) {
                                                logger.error("Setting of mongo_id failed", e);
                                            }
                                        }
                                    }
                                }

                            } else if (dbLst.size() > 0) {
                                doStoreList(dbLst, wc, collection);
                                for (DBObject o : dbLst) {
                                    Object entity = mapMarshalledNewObjects.get(dbLst.indexOf(o));
                                    if (entity == null) {
                                        logger.error("cannot update mongo_id...");
                                    } else {
                                        try {
                                            morphium.getARHelper().getIdField(entity).set(entity, o.get("_id"));
                                        } catch (Exception e) {
                                            logger.error("Setting of mongo_id failed", e);
                                        }
                                    }
                                }
                            }
                            morphium.getCache().clearCacheIfNecessary(c);
                            long dur = System.currentTimeMillis() - start;
                            //bulk insert
                            morphium.fireProfilingWriteEvent(c, dbLst, dur, true, WriteAccessType.BULK_INSERT);
                            for (Object record : es.getValue()) {
                                if (isNew.get(record) == null || isNew.get(record)) { //null because key changed => mongo _id
                                    morphium.firePostStoreEvent(record, true);
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


    private void createCappedColl(Class c) {
        if (logger.isDebugEnabled())
            logger.debug("Collection does not exist - ensuring indices / capped status");
        BasicDBObject cmd = new BasicDBObject();
        cmd.put("create", morphium.getMapper().getCollectionName(c));
        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
        if (capped != null) {
            cmd.put("capped", true);
            cmd.put("size", capped.maxSize());
            cmd.put("max", capped.maxEntries());
        } else {
//            logger.warn("cannot cap collection for class " + c.getName() + " not @Capped");
            return;
        }
        cmd.put("autoIndexId", (morphium.getARHelper().getIdField(c).getType().equals(ObjectId.class)));
        morphium.getDatabase().command(cmd);
    }

    private void doStoreList(List<DBObject> dbLst, WriteConcern wc, DBCollection collection) {
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                if (wc == null) {
                    collection.insert(dbLst);
                } else {
                    collection.insert(dbLst, wc);
                }
                break;
            } catch (Exception e) {
                morphium.handleNetworkError(i, e);
            }
        }
    }


    /**
     * automatically convert the collection for the given type to a capped collection
     * only works if @Capped annotation is given for type
     *
     * @param c - type
     */
    public <T> void convertToCapped(final Class<T> c) {
        convertToCapped(c, null);
    }

    public <T> void convertToCapped(final Class<T> c, final AsyncOperationCallback<T> callback) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                WriteConcern wc = morphium.getWriteConcernForClass(c);
                String coll = morphium.getMapper().getCollectionName(c);
                DBCollection collection = null;

                for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                    try {
                        collection = morphium.getDatabase().getCollection(coll);
                        if (!morphium.getDatabase().collectionExists(coll)) {
                            if (logger.isDebugEnabled())
                                logger.debug("Collection does not exist - ensuring indices / capped status");
                            BasicDBObject cmd = new BasicDBObject();
                            cmd.put("create", coll);
                            Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
                            if (capped != null) {
                                cmd.put("capped", true);
                                cmd.put("size", capped.maxSize());
                                cmd.put("max", capped.maxEntries());
                            }
                            cmd.put("autoIndexId", (morphium.getARHelper().getIdField(c).getType().equals(ObjectId.class)));
                            morphium.getDatabase().command(cmd);
                        } else {
                            Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
                            if (capped != null) {
                                BasicDBObject cmd = new BasicDBObject();
                                cmd.put("convertToCapped", coll);
                                cmd.put("size", capped.maxSize());
                                cmd.put("max", capped.maxEntries());
                                morphium.getDatabase().command(cmd);
                                //Indexes are not available after converting - recreate them
                                morphium.ensureIndicesFor(c, callback);
                            }
                        }
                        break;
                    } catch (Throwable t) {
                        morphium.handleNetworkError(i, t);
                    }
                }
            }
        };

        if (callback == null) {
            r.run();
        } else {
            morphium.getAsyncOperationsThreadPool().execute(r);
//            new Thread(r).start();
        }
    }


    private void executeWriteBatch(List<Object> es, Class c, WriteConcern wc, BulkWriteOperation bulkWriteOperation, long start) {
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                BulkWriteResult result;
                if (wc == null) {
                    result = bulkWriteOperation.execute();
                } else {
                    result = bulkWriteOperation.execute(wc);
                }
                //TODO: to something with the result
                break;
            } catch (Exception e) {
                morphium.handleNetworkError(i, e);
            }
            long dur = System.currentTimeMillis() - start;
            morphium.fireProfilingWriteEvent(c, es, dur, false, WriteAccessType.BULK_UPDATE);
            morphium.getCache().clearCacheIfNecessary(c);
            morphium.firePostStoreEvent(es, false);
        }
    }

    @Override
    public <T> void set(final T toSet, final String collection, final String field, final Object v, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask<T> r = new WriterTask<T>() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toSet.getClass();
                Object value = v;
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                value = marshallIfNecessary(value);
                if (collection == null) {
                    morphium.getMapper().getCollectionName(cls);
                }
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$set", new BasicDBObject(fieldName, value.getClass().isEnum() ? value.toString() : value));
                List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                if (lastChangeFields != null && lastChangeFields.size() != 0) {
                    for (String fL : lastChangeFields) {
                        Field fld = morphium.getARHelper().getField(cls, fL);
                        if (fld.getType().equals(Date.class)) {
                            ((BasicDBObject) update.get("$set")).put(fL, new Date());
                        } else {
                            ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                        }
                    }
                }

                List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
                if (insertIfNotExist && creationTimeFields != null && creationTimeFields.size() != 0) {
                    long cnt = morphium.getDatabase().getCollection(collection).count(query);
                    if (cnt == 0) {
                        //not found, would insert
                        for (String fL : creationTimeFields) {
                            Field fld = morphium.getARHelper().getField(cls, fL);
                            if (fld.getType().equals(Date.class)) {
                                ((BasicDBObject) update.get("$set")).put(fL, new Date());
                            } else {
                                ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                            }
                        }
                    }
                }


                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();

                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(collection)) {
                                morphium.ensureIndicesFor(cls, collection, callback);
                            }

                            if (wc == null) {
                                morphium.getDatabase().getCollection(collection).update(query, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(collection).update(query, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
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
                morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    public <T> void submitAndBlockIfNecessary(AsyncOperationCallback<T> callback, WriterTask<T> r) {
        if (callback == null) {
            r.run();
        } else {
            r.setCallback(callback);
            int tries = 0;
            boolean retry = true;
            while (retry) {
                try {
                    tries++;
                    executor.submit(r);
                    retry = false;
//                } catch (OutOfMemoryError ignored) {
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    if (tries > maximumRetries) {
                        throw new RuntimeException("Could not write - not even after " + maximumRetries + " and pause of " + pause + "ms", e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.warn("thread pool exceeded - waiting");
                    }
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException ignored) {

                    }

                }
            }
        }
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, AsyncOperationCallback<T> callback, final String... fields) {
        if (ent == null) return;
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(ent);
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
                        Object value = morphium.getARHelper().getValue(ent, f);
                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class)) {
                            if (morphium.getARHelper().getField(ent.getClass(), f).getAnnotation(Reference.class) != null) {
                                //need to store reference
                                value = morphium.getARHelper().getId(ent);
                            } else {
                                value = morphium.getMapper().marshall(value);
                            }
                        }
                        update.put(f, value);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                Class<?> type = morphium.getARHelper().getRealClass(ent.getClass());

                LastChange t = morphium.getARHelper().getAnnotationFromHierarchy(type, LastChange.class); //(StoreLastChange) type.getAnnotation(StoreLastChange.class);
                if (t != null) {
                    List<String> lst = morphium.getARHelper().getFields(ent.getClass(), LastChange.class);

                    long now = System.currentTimeMillis();
                    for (String ctf : lst) {
                        Field f = morphium.getARHelper().getField(type, ctf);
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

                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(collectionName)) {
                                morphium.ensureIndicesFor((Class<T>) ent.getClass(), collectionName, callback);
                            }
                            if (wc != null) {
                                morphium.getDatabase().getCollection(collectionName).update(find, update, false, false, wc);
                            } else {
                                morphium.getDatabase().getCollection(collectionName).update(find, update, false, false);
                            }
                            break;
                        } catch (Throwable th) {
                            morphium.handleNetworkError(i, th);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(ent.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(morphium.getARHelper().getRealClass(ent.getClass()));
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
    public <T> void remove(final List<T> lst, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                HashMap<Class<T>, List<Query<T>>> sortedMap = new HashMap<>();

                for (T o : lst) {
                    if (sortedMap.get(o.getClass()) == null) {
                        List<Query<T>> queries = new ArrayList<>();
                        sortedMap.put((Class<T>) o.getClass(), queries);
                    }
                    Query<T> q = (Query<T>) morphium.createQueryFor(o.getClass());
                    q.f(morphium.getARHelper().getIdFieldName(o)).eq(morphium.getARHelper().getId(o));
                    sortedMap.get(o.getClass()).add(q);
                }
                morphium.firePreRemove(lst);
                long start = System.currentTimeMillis();
                try {
                    for (Class<T> cls : sortedMap.keySet()) {
                        Query<T> orQuery = morphium.createQueryFor(cls);
                        orQuery = orQuery.or(sortedMap.get(cls));
                        remove(orQuery, null); //sync call
                    }
                    morphium.firePostRemove(lst);
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
    public <T> void remove(final Query<T> q, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                morphium.firePreRemoveEvent(q);
                WriteConcern wc = morphium.getWriteConcernForClass(q.getType());
                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            String collectionName = q.getCollectionName();

                            if (wc == null) {
                                morphium.getDatabase().getCollection(collectionName).remove(q.toQueryObject());
                            } else {
                                morphium.getDatabase().getCollection(collectionName).remove(q.toQueryObject(), wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
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
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> callback) {

        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(o);
                morphium.firePreRemove(o);
                BasicDBObject db = new BasicDBObject();
                db.append("_id", id);
                WriteConcern wc = morphium.getWriteConcernForClass(o.getClass());

                long start = System.currentTimeMillis();
                try {
                    if (collection == null) {
                        morphium.getMapper().getCollectionName(o.getClass());
                    }
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (wc == null) {
                                morphium.getDatabase().getCollection(collection).remove(db);
                            } else {
                                morphium.getDatabase().getCollection(collection).remove(db, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
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
    public <T> void inc(final T toInc, final String collection, final String field, final Number amount, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toInc.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(toInc));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
                WriteConcern wc = morphium.getWriteConcernForClass(toInc.getClass());

                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }

                    morphium.getCache().clearCacheIfNecessary(cls);

                    if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                        try {
                            f.set(toInc, ((Integer) f.get(toInc)) + (int) amount.intValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                        try {
                            f.set(toInc, ((Double) f.get(toInc)) + amount.doubleValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                        try {
                            f.set(toInc, ((Float) f.get(toInc)) + (float) amount.floatValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                        try {
                            f.set(toInc, ((Long) f.get(toInc)) + (long) amount.longValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        logger.error("Could not set increased value - unsupported type " + cls.getName());
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
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
    public <T> void inc(final Query<T> query, final Map<String, Number> fieldsToInc, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<? extends T> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();

                BasicDBObject update = new BasicDBObject();
                update.put("$inc", new BasicDBObject(fieldsToInc));
                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                            }
                            WriteConcern wc = morphium.getWriteConcernForClass(cls);
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.INC, query, System.currentTimeMillis() - start, null, null, fieldsToInc);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.INC, query, System.currentTimeMillis() - start, e.getMessage(), e, null, fieldsToInc);

                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void inc(final Query<T> query, final String field, final Number amount, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();
                String fieldName = morphium.getARHelper().getFieldName(cls, field);
                BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            WriteConcern wc = morphium.getWriteConcernForClass(cls);
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
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
    public <T> void set(final Query<T> query, final Map<String, Object> values, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        submitAndBlockIfNecessary(callback, new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                BasicDBObject toSet = new BasicDBObject();
                for (Map.Entry<String, Object> ef : values.entrySet()) {
                    String fieldName = morphium.getARHelper().getFieldName(cls, ef.getKey());
                    toSet.put(fieldName, marshallIfNecessary(ef.getValue()));
                }
                BasicDBObject update = new BasicDBObject("$set", toSet);
                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                    List<String> creationTimeFlds = morphium.getARHelper().getFields(cls, CreationTime.class);
                    if (creationTimeFlds != null && creationTimeFlds.size() != 0 && morphium.getDatabase().getCollection(coll).find(qobj).count() == 0) {
                        if (creationTimeFlds != null && creationTimeFlds.size() != 0) {
                            for (String fL : creationTimeFlds) {
                                Field fld = morphium.getARHelper().getField(cls, fL);
                                if (fld.getType().equals(Date.class)) {
                                    ((BasicDBObject) update.get("$set")).put(fL, new Date());
                                } else {
                                    ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                                }
                            }
                        }
                    }
                }


                List<String> latChangeFlds = morphium.getARHelper().getFields(cls, LastChange.class);
                if (latChangeFlds != null && latChangeFlds.size() != 0) {
                    for (String fL : latChangeFlds) {
                        Field fld = morphium.getARHelper().getField(cls, fL);
                        if (fld.getType().equals(Date.class)) {
                            ((BasicDBObject) update.get("$set")).put(fL, new Date());
                        } else {
                            ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                        }
                    }
                }


                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, values, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, values, insertIfNotExist, multiple);
                }
            }
        });
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> callback, final boolean multiple, final Enum... fields) {
        ArrayList<String> flds = new ArrayList<>();
        for (Enum e : fields) {
            flds.add(e.name());
        }
        unset(query, callback, multiple, flds.toArray(new String[fields.length]));
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> callback, final boolean multiple, final String... fields) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                DBObject qobj = query.toQueryObject();

                Map<String, String> toSet = new HashMap<>();
                for (String f : fields) {
                    toSet.put(f, ""); //value is ignored
                }
                BasicDBObject update = new BasicDBObject("$unset", toSet);
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, false, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, false, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, false, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, fields, false, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, fields, false, multiple);
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
    public <T> void unset(final T toSet, final String collection, final String field, AsyncOperationCallback<T> callback) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");
        if (morphium.getARHelper().getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet, collection, callback);
        }

        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toSet.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$unset", new BasicDBObject(fieldName, 1));
                WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());

                long start = System.currentTimeMillis();

                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }

                        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                        if (lastChangeFields != null && lastChangeFields.size() != 0) {
                            update = new BasicDBObject("$set", new BasicDBObject());
                            for (String fL : lastChangeFields) {
                                Field fld = morphium.getARHelper().getField(cls, fL);
                                if (fld.getType().equals(Date.class)) {
                                    ((BasicDBObject) update.get("$set")).put(fL, new Date());
                                } else {
                                    ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                                }
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }

                        }

                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(toSet.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);

                    try {
                        f.set(toSet, null);
                    } catch (IllegalAccessException e) {
                        //May happen, if null is not allowed for example
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);
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
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> callback) {
        if (obj == null) throw new RuntimeException("Cannot update null!");
        if (morphium.getARHelper().getId(obj) == null) {
            logger.info("just storing object as it is new...");
            store(obj, collection, callback);
        }

        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = obj.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                BasicDBObject query = new BasicDBObject();
                query.put("_id", morphium.getId(obj));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                BasicDBObject update = new BasicDBObject("$pop", new BasicDBObject(fieldName, first ? -1 : 1));
                WriteConcern wc = morphium.getWriteConcernForClass(obj.getClass());

                long start = System.currentTimeMillis();

                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }

                        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                        if (lastChangeFields != null && lastChangeFields.size() != 0) {
                            update = new BasicDBObject("$set", new BasicDBObject());
                            for (String fL : lastChangeFields) {
                                Field fld = morphium.getARHelper().getField(cls, fL);
                                if (fld.getType().equals(Date.class)) {
                                    ((BasicDBObject) update.get("$set")).put(fL, new Date());
                                } else {
                                    ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                                }
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }

                        }

                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(obj.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);

                    try {
                        f.set(obj, null);
                    } catch (IllegalAccessException e) {
                        //May happen, if null is not allowed for example
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, null, obj, field);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, e.getMessage(), e, obj, field);
                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void unset(Query<T> query, String field, boolean multiple, AsyncOperationCallback<T> callback) {
        unset(query, callback, multiple, field);
    }

    @Override
    public <T> void pushPull(final boolean push, final Query<T> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

                String coll = morphium.getMapper().getCollectionName(cls);

                DBObject qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }
                Object v = marshallIfNecessary(value);

                String fieldName = morphium.getARHelper().getFieldName(cls, field);
                BasicDBObject set = new BasicDBObject(fieldName, v.getClass().isEnum() ? v.toString() : v);
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
            if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class)
                    || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
                //need to marshall...
                DBObject marshall = morphium.getMapper().marshall(value);
                marshall.put("class_name", morphium.getARHelper().getRealClass(value.getClass()).getName());
                value = marshall;
            } else if (List.class.isAssignableFrom(value.getClass())) {
                List lst = new ArrayList();
                for (Object o : (List) value) {
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Embedded.class) ||
                            morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Entity.class)
                            ) {
                        DBObject marshall = morphium.getMapper().marshall(o);
                        marshall.put("class_name", morphium.getARHelper().getRealClass(o.getClass()).getName());

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
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Entity.class) ||
                            morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Embedded.class)
                            ) {
                        DBObject marshall = morphium.getMapper().marshall(en.getValue());
                        marshall.put("class_name", morphium.getARHelper().getRealClass(en.getValue().getClass()).getName());
                        ((Map) value).put(en.getKey(), marshall);
                    }
                }

            }
        }
        return value;
    }

    private void pushIt(boolean push, boolean insertIfNotExist, boolean multiple, Class<?> cls, String coll, DBObject qobj, BasicDBObject update) {
        morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
        if (lastChangeFields != null && lastChangeFields.size() != 0) {
            update.put("$set", new BasicDBObject());
            for (String fL : lastChangeFields) {
                Field fld = morphium.getARHelper().getField(cls, fL);

                if (fld.getType().equals(Date.class)) {

                    ((BasicDBObject) update.get("$set")).put(fL, new Date());
                } else {
                    ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                }
            }
        }
        if (insertIfNotExist) {
            List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
            if (insertIfNotExist && creationTimeFields != null && creationTimeFields.size() != 0) {
                long cnt = morphium.getDatabase().getCollection(coll).count(qobj);
                if (cnt == 0) {
                    //not found, would insert
                    if (update.get("$set") == null) {
                        update.put("$set", new BasicDBObject());
                    }
                    for (String fL : creationTimeFields) {
                        Field fld = morphium.getARHelper().getField(cls, fL);

                        if (fld.getType().equals(Date.class)) {
                            ((BasicDBObject) update.get("$set")).put(fL, new Date());
                        } else {
                            ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                        }
                    }
                }
            }
        }

        WriteConcern wc = morphium.getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                if (!morphium.getDatabase().collectionExists(coll) && insertIfNotExist) {
                    morphium.ensureIndicesFor(cls, coll);
                }
                if (wc == null) {
                    morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                } else {
                    morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                }
                break;
            } catch (Exception e) {
                morphium.handleNetworkError(i, e);
            }
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
    }

    @Override
    public <T> void pushPullAll(final boolean push, final Query<T> query, final String f, final List<?> v, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                List<?> value = v;
                String field = f;
                Class<?> cls = query.getType();
                String coll = morphium.getMapper().getCollectionName(cls);
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
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

                    field = morphium.getARHelper().getFieldName(cls, field);
                    BasicDBObject set = new BasicDBObject(field, value);
                    BasicDBObject update = new BasicDBObject(push ? "$pushAll" : "$pullAll", set);

                    List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                    if (lastChangeFields != null && lastChangeFields.size() != 0) {
                        update.put("$set", new BasicDBObject());
                        for (String fL : lastChangeFields) {
                            Field fld = morphium.getARHelper().getField(cls, fL);

                            if (fld.getType().equals(Date.class)) {

                                ((BasicDBObject) update.get("$set")).put(fL, new Date());
                            } else {
                                ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                            }
                        }
                    }
                    if (insertIfNotExist) {
                        List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
                        if (insertIfNotExist && creationTimeFields != null && creationTimeFields.size() != 0) {
                            long cnt = morphium.getDatabase().getCollection(coll).count(qobj);
                            if (cnt == 0) {
                                //not found, would insert
                                if (update.get("$set") == null) {
                                    update.put("$set", new BasicDBObject());
                                }
                                for (String fL : creationTimeFields) {
                                    Field fld = morphium.getARHelper().getField(cls, fL);

                                    if (fld.getType().equals(Date.class)) {
                                        ((BasicDBObject) update.get("$set")).put(fL, new Date());
                                    } else {
                                        ((BasicDBObject) update.get("$set")).put(fL, System.currentTimeMillis());
                                    }
                                }
                            }
                        }
                    }

                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
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
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class)) {
            WriterTask r = new WriterTask() {
                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                public void run() {
                    morphium.firePreDropEvent(cls);
                    long start = System.currentTimeMillis();
                    String co = collection;
                    if (co == null) {
                        co = morphium.getMapper().getCollectionName(cls);
                    }
                    DBCollection coll = morphium.getDatabase().getCollection(co);
//            coll.setReadPreference(com.mongodb.ReadPreference.PRIMARY);
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            coll.drop();
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
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
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            public void run() {
                List<String> fields = morphium.getARHelper().getFields(cls);

                Map<String, Object> idx = new LinkedHashMap<>();
                for (Map.Entry<String, Object> es : index.entrySet()) {
                    String k = es.getKey();
                    if (!k.contains(".") && !fields.contains(k) && !fields.contains(morphium.getARHelper().convertCamelCase(k))) {
                        throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
                    }
                    String fn = morphium.getARHelper().getFieldName(cls, k);
                    idx.put(fn, es.getValue());
                }
                long start = System.currentTimeMillis();
                BasicDBObject keys = new BasicDBObject(idx);
                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                    try {
                        if (options == null) {
                            morphium.getDatabase().getCollection(coll).createIndex(keys);
                        } else {
                            BasicDBObject opts = new BasicDBObject(options);
                            morphium.getDatabase().getCollection(coll).createIndex(keys, opts);
                        }
                    } catch (Exception e) {
                        morphium.handleNetworkError(i, e);
                    }
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

    @Override
    public void onShutdown(Morphium m) {
        if (executor != null) {
            try {
                executor.shutdownNow();
            } catch (Exception e) {
                //swallow
            }
        }
    }
}
