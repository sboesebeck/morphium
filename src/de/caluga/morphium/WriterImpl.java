package de.caluga.morphium;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 14:38
 * <p/>
 * default writer implementation
 *
 * @see Writer
 */
@SuppressWarnings({"ConstantConditions", "unchecked"})
public class WriterImpl implements Writer {
    private static Logger logger = Logger.getLogger(WriterImpl.class);
    private Morphium morphium;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
    }


    /**
     * @param o - object to store
     */
    @Override
    public void store(Object o) {
        if (o instanceof List) {
            store((List) o);
        }
        long start = System.currentTimeMillis();
        Class type = annotationHelper.getRealClass(o.getClass());
        if (!annotationHelper.isAnnotationPresentInHierarchy(type, Entity.class)) {
            throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
        }
        morphium.inc(StatisticKeys.WRITES);
        ObjectId id = annotationHelper.getId(o);
        if (annotationHelper.isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
            if ((o instanceof PartiallyUpdateable)) {
                morphium.updateUsingFields(o, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
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
                        Field f = morphium.getField(type, ctf);
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
                lst = annotationHelper.getFields(type, CreatedBy.class);
                if (lst != null && lst.size() > 0) {
                    for (String ctf : lst) {

                        Field f = morphium.getField(type, ctf);
                        if (f != null) {
                        }
                    }
                }
            }
        }
        if (annotationHelper.isAnnotationPresentInHierarchy(type, LastChange.class)) {
            List<String> lst = annotationHelper.getFields(type, LastChange.class);
            if (lst != null && lst.size() > 0) {
                for (String ctf : lst) {
                    long now = System.currentTimeMillis();
                    Field f = morphium.getField(type, ctf);
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

            lst = annotationHelper.getFields(type, LastChangeBy.class);
            if (lst != null && lst.size() > 0) {
                for (String ctf : lst) {

                    Field f = morphium.getField(type, ctf);
                    if (f != null) {
                    }
                }
            }
        }

        String coll = morphium.getMapper().getCollectionName(type);
        if (!morphium.getDatabase().collectionExists(coll)) {
            if (logger.isDebugEnabled())
                logger.debug("Collection does not exist - ensuring indices");
            morphium.ensureIndicesFor(type);
        }

        WriteConcern wc = morphium.getWriteConcernForClass(type);
        if (wc != null) {
            morphium.getDatabase().getCollection(coll).save(marshall, wc);
        } else {

            morphium.getDatabase().getCollection(coll).save(marshall);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(o.getClass(), marshall, dur, true, WriteAccessType.SINGLE_INSERT);
        if (logger.isDebugEnabled()) {
            String n = "";
            if (isNew) {
                n = "NEW ";
            }
            logger.debug(n + "stored " + type.getSimpleName() + " after " + dur + " ms length:" + marshall.toString().length());
        }
        if (isNew) {
            List<String> flds = annotationHelper.getFields(o.getClass(), Id.class);
            if (flds == null) {
                throw new RuntimeException("Object does not have an ID field!");
            }
            try {
                //Setting new ID (if object was new created) to Entity
                morphium.getField(o.getClass(), flds.get(0)).set(o, marshall.get("_id"));
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
    }

    @Override
    public void store(List lst) {
        if (!lst.isEmpty()) {
            HashMap<Class, List<Object>> sorted = new HashMap<Class, List<Object>>();
            HashMap<Object, Boolean> isNew = new HashMap<Object, Boolean>();
            for (Object o : lst) {
                Class type = annotationHelper.getRealClass(o.getClass());
                if (!annotationHelper.isAnnotationPresentInHierarchy(type, Entity.class)) {
                    logger.error("Not an entity! Storing not possible! Even not in list!");
                    continue;
                }
                morphium.inc(StatisticKeys.WRITES);
//                ObjectId id = morphium.getMapper().getId(o);
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

//            firePreListStoreEvent(lst,isNew);
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
                        if (wc == null) {
                            collection.save(marshall);
                        } else {
                            collection.save(marshall, wc);
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
//            firePostListStoreEvent(lst,isNew);
        }
    }

    /**
     * changes an object in DB
     *
     * @param toSet - entity
     * @param field - field to set
     * @param value - value to set field to
     */
    @Override
    public void set(Object toSet, String field, Object value) {
        Class cls = toSet.getClass();
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
        value = marshallIfNecessary(value);
        String coll = morphium.getMapper().getCollectionName(cls);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", morphium.getId(toSet));
        Field f = morphium.getField(cls, field);
        if (f == null) {
            throw new RuntimeException("Unknown field: " + field);
        }
        String fieldName = morphium.getFieldName(cls, field);

        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject(fieldName, value));

        WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());
        long start = System.currentTimeMillis();
        if (wc == null) {
            morphium.getDatabase().getCollection(coll).update(query, update);
        } else {
            morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, false, WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        try {
            f.set(toSet, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
    }

    @Override
    public void storeUsingFields(Object ent, String... fields) {
        ObjectId id = morphium.getId(ent);
        if (ent == null) return;
        if (id == null) {
            //new object - update not working
            logger.warn("trying to partially update new object - storing it in full!");
            store(ent);
            return;
        }
        morphium.firePreStoreEvent(ent, false);
        morphium.inc(StatisticKeys.WRITES);
        DBObject find = new BasicDBObject();

        find.put("_id", id);
        DBObject update = new BasicDBObject();
        for (String f : fields) {
            try {
                Object value = morphium.getValue(ent, f);
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
                Field f = morphium.getField(type, ctf);
                if (f != null) {
                    try {
                        f.set(ent, now);
                    } catch (IllegalAccessException e) {
                        logger.error("Could not set modification time", e);

                    }
                }
                update.put(ctf, now);
            }
            lst = annotationHelper.getFields(ent.getClass(), LastChangeBy.class);
            if (lst != null && lst.size() != 0) {
                for (String ctf : lst) {

                    Field f = morphium.getField(type, ctf);

                }
            }
        }


        update = new BasicDBObject("$set", update);
        WriteConcern wc = morphium.getWriteConcernForClass(type);
        long start = System.currentTimeMillis();
        if (wc != null) {
            morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(ent.getClass())).update(find, update, false, false, wc);
        } else {
            morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(ent.getClass())).update(find, update, false, false);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(ent.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(annotationHelper.getRealClass(ent.getClass()));
        morphium.firePostStoreEvent(ent, false);
    }


    @Override
    public void delete(List lst) {
        for (Object o : lst) {
            delete(o);
        }
    }

    /**
     * deletes all objects matching the given query
     *
     * @param q - query
     */
    @Override
    public void delete(Query q) {
        morphium.firePreRemoveEvent(q);
        WriteConcern wc = morphium.getWriteConcernForClass(q.getType());
        long start = System.currentTimeMillis();
        if (wc == null) {
            morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject());
        } else {
            morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject(), wc);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(q.getType(), q.toQueryObject(), dur, false, WriteAccessType.BULK_DELETE);
        morphium.getCache().clearCacheIfNecessary(q.getType());
        morphium.firePostRemoveEvent(q);
    }

    @Override
    public void delete(Object o) {
        if (o instanceof List) {
            delete((List) o);
            return;
        }
        if (o instanceof Query) {
            delete((Query) o);
            return;
        }
        ObjectId id = annotationHelper.getId(o);
        BasicDBObject db = new BasicDBObject();
        db.append("_id", id);
        WriteConcern wc = morphium.getWriteConcernForClass(o.getClass());

        long start = System.currentTimeMillis();
        if (wc == null) {
            morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).remove(db);
        } else {
            morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).remove(db, wc);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(o.getClass(), o, dur, false, WriteAccessType.SINGLE_DELETE);
        morphium.clearCachefor(o.getClass());
        morphium.inc(StatisticKeys.WRITES);
        morphium.firePostRemoveEvent(o);
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
    public void inc(Object toInc, String field, int amount) {

        Class cls = toInc.getClass();
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
        String coll = morphium.getMapper().getCollectionName(cls);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", morphium.getId(toInc));
        Field f = morphium.getField(cls, field);
        if (f == null) {
            throw new RuntimeException("Unknown field: " + field);
        }
        String fieldName = morphium.getFieldName(cls, field);

        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
        WriteConcern wc = morphium.getWriteConcernForClass(toInc.getClass());
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


    }

    @Override
    public void inc(Query<?> query, String field, int amount, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
        String coll = morphium.getMapper().getCollectionName(cls);
        String fieldName = morphium.getFieldName(cls, field);
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
        if (wc == null) {
            morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
        } else {
            morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
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
    public void set(Query<?> query, Map<String, Object> values, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        String coll = morphium.getMapper().getCollectionName(cls);
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
        BasicDBObject toSet = new BasicDBObject();
        for (Map.Entry<String, Object> ef : values.entrySet()) {
            String fieldName = morphium.getFieldName(cls, ef.getKey());
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
        if (wc == null) {
            morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
        } else {
            morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
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
    public void unset(Object toSet, String field) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");
        if (morphium.getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
        }
        Class cls = toSet.getClass();
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

        String coll = morphium.getMapper().getCollectionName(cls);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", morphium.getId(toSet));
        Field f = morphium.getField(cls, field);
        if (f == null) {
            throw new RuntimeException("Unknown field: " + field);
        }
        String fieldName = morphium.getFieldName(cls, field);

        BasicDBObject update = new BasicDBObject("$unset", new BasicDBObject(fieldName, 1));
        WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());
        if (!morphium.getDatabase().collectionExists(coll)) {
            morphium.ensureIndicesFor(cls);
        }
        long start = System.currentTimeMillis();
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

    }


    @Override
    public void pushPull(boolean push, Query<?> query, String field, Object value, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

        String coll = morphium.getMapper().getCollectionName(cls);

        DBObject qobj = query.toQueryObject();
        if (insertIfNotExist) {
            qobj = morphium.simplifyQueryObject(qobj);
        }
        value = marshallIfNecessary(value);

        field = annotationHelper.getFieldName(cls, field);
        BasicDBObject set = new BasicDBObject(field, value);
        BasicDBObject update = new BasicDBObject(push ? "$push" : "$pull", set);

        pushIt(push, insertIfNotExist, multiple, cls, coll, qobj, update);

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
    public void pushPullAll(boolean push, Query<?> query, String field, List<?> value, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        String coll = morphium.getMapper().getCollectionName(cls);
        morphium.firePreUpdateEvent(annotationHelper.getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
        List lst = new ArrayList();
        for (Object o : value) {
            lst.add(marshallIfNecessary(o));
        }
        value = lst;
//        BasicDBList dbl = new BasicDBList();
//        dbl.addAll(value);

        DBObject qobj = query.toQueryObject();
        if (insertIfNotExist) {
            qobj = morphium.simplifyQueryObject(qobj);
        }
        field = annotationHelper.getFieldName(cls, field);
        BasicDBObject set = new BasicDBObject(field, value);
        BasicDBObject update = new BasicDBObject(push ? "$pushAll" : "$pullAll", set);
        pushIt(push, insertIfNotExist, multiple, cls, coll, qobj, update);
    }

}
