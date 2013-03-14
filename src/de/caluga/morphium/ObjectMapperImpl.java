package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 19:36
 * <p/>
 */
@SuppressWarnings({"ConstantConditions", "MismatchedQueryAndUpdateOfCollection", "unchecked", "MismatchedReadAndWriteOfArray"})
public class ObjectMapperImpl implements ObjectMapper {
    private static Logger log = Logger.getLogger(ObjectMapperImpl.class);
    private volatile Hashtable<Class<?>, NameProvider> nameProviders;
    private volatile AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();
    private Morphium morphium;

    public ObjectMapperImpl() {

        nameProviders = new Hashtable<Class<?>, NameProvider>();
    }

    /**
     * will automatically be called after instanciation by Morphium
     * also gets the AnnotationAndReflectionHelper from this object (to make use of the caches)
     *
     * @param m - the Morphium instance
     */
    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        if (m != null) {
            annotationHelper = m.getARHelper();
        } else {
            annotationHelper = new AnnotationAndReflectionHelper();
        }
    }


    /**
     * override nameprovider in runtime!
     *
     * @param cls - class to use
     * @param np  - the NameProvider for that class
     */
    public void setNameProviderForClass(Class<?> cls, NameProvider np) {
        nameProviders.put(cls, np);
    }

    public NameProvider getNameProviderForClass(Class<?> cls) {
        Entity e = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class);
        if (e == null) {
            throw new IllegalArgumentException("no entity annotation found");
        }
        try {
            return getNameProviderForClass(cls, e);
        } catch (Exception ex) {
            log.error("Error getting nameProvider", ex);
            throw new IllegalArgumentException("could not get name provicer", ex);
        }
    }

    private NameProvider getNameProviderForClass(Class<?> cls, Entity p) throws IllegalAccessException, InstantiationException {
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }

        if (nameProviders.get(cls) == null) {
            NameProvider np = p.nameProvider().newInstance();
            nameProviders.put(cls, np);
        }
        return nameProviders.get(cls);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String getCollectionName(Class cls) {
        Entity p = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) cls.getAnnotation(Entity.class);
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }
        try {
            cls = annotationHelper.getRealClass(cls);
            NameProvider np = getNameProviderForClass(cls, p);
            return np.getCollectionName(cls, this, p.translateCamelCase(), p.useFQN(), p.collectionName().equals(".") ? null : p.collectionName(), morphium);
        } catch (InstantiationException e) {
            log.error("Could not instanciate NameProvider: " + p.nameProvider().getName(), e);
            throw new RuntimeException("Could not Instaciate NameProvider", e);
        } catch (IllegalAccessException e) {
            log.error("Illegal Access during instanciation of NameProvider: " + p.nameProvider().getName(), e);
            throw new RuntimeException("Illegal Access during instanciation", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public DBObject marshall(Object o) {
        //recursively map object ot mongo-Object...
        if (!annotationHelper.isEntity(o)) {
            throw new IllegalArgumentException("Object is no entity: " + o.getClass().getSimpleName());
        }

        DBObject dbo = new BasicDBObject();
        if (o == null) {
            return dbo;
        }
        Class<?> cls = annotationHelper.getRealClass(o.getClass());
        if (cls == null) {
            throw new IllegalArgumentException("No real class?");
        }
        o = annotationHelper.getRealObject(o);
        List<String> flds = annotationHelper.getFields(cls);
        if (flds == null) {
            throw new IllegalArgumentException("Fields not found? " + cls.getName());
        }
        Entity e = annotationHelper.getAnnotationFromHierarchy(o.getClass(), Entity.class); //o.getClass().getAnnotation(Entity.class);
        Embedded emb = annotationHelper.getAnnotationFromHierarchy(o.getClass(), Embedded.class); //o.getClass().getAnnotation(Embedded.class);

        if (e != null && e.polymorph()) {
            dbo.put("class_name", cls.getName());
        }

        if (emb != null && emb.polymorph()) {
            dbo.put("class_name", cls.getName());
        }

        for (String f : flds) {
            String fName = f;
            try {
                Field fld = annotationHelper.getField(cls, f);
                if (fld == null) {
                    log.error("Field not found");
                    continue;
                }
                //do not store static fields!
                if (Modifier.isStatic(fld.getModifiers())) {
                    continue;
                }
                AdditionalData ad = fld.getAnnotation(AdditionalData.class);
                if (ad != null) {
                    if (!ad.readOnly()) {
                        //storing additional data
                        dbo.putAll((Map<String, Object>) fld.get(o));
                    }
                    //additional data is usually transient
                    continue;
                }
                if (fld.isAnnotationPresent(Id.class)) {
                    fName = "_id";
                }
                Object v = null;
                Object value = fld.get(o);
                if (fld.isAnnotationPresent(Reference.class)) {
                    Reference r = fld.getAnnotation(Reference.class);
                    //reference handling...
                    //field should point to a certain type - store ObjectID only
                    if (value == null) {
                        //no reference to be stored...
                        v = null;
                    } else {
                        if (Collection.class.isAssignableFrom(fld.getType())) {
                            //list of references....
                            BasicDBList lst = new BasicDBList();
                            for (Object rec : ((Collection) value)) {
                                if (rec != null) {
                                    ObjectId id = annotationHelper.getId(rec);
                                    if (id == null) {
                                        if (r.automaticStore()) {
                                            if (morphium == null) {
                                                throw new RuntimeException("Could not automagically store references as morphium is not set!");
                                            }
                                            morphium.storeNoCache(rec);
                                            id = annotationHelper.getId(rec);
                                        } else {
                                            throw new IllegalArgumentException("Cannot store reference to unstored entity if automaticStore in @Reference is set to false!");
                                        }
                                    }
                                    if (morphium == null) {
                                        throw new RuntimeException("cannot set dbRef - morphium is not set");
                                    }
                                    DBRef ref = new DBRef(morphium.getDatabase(), annotationHelper.getRealClass(rec.getClass()).getName(), id);
                                    lst.add(ref);
                                } else {
                                    lst.add(null);
                                }
                            }
                            v = lst;
                        } else if (Map.class.isAssignableFrom(fld.getType())) {
                            throw new RuntimeException("Cannot store references in Maps!");
                        } else {

                            if (annotationHelper.getId(value) == null) {
                                //not stored yet
                                if (r.automaticStore()) {
                                    //TODO: this could cause an endless loop!
                                    if (morphium == null) {
                                        log.fatal("Could not store - no Morphium set!");
                                    } else {
                                        morphium.storeNoCache(value);
                                    }
                                } else {
                                    throw new IllegalArgumentException("Reference to be stored, that is null!");
                                }


                            }
                            //DBRef ref = new DBRef(morphium.getDatabase(), value.getClass().getName(), getId(value));
                            v = annotationHelper.getId(value);
                        }
                    }
                } else {

                    //check, what type field has

                    //Store Entities recursively
                    //TODO: Fix recursion - this could cause a loop!
                    if (annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Entity.class)) {
                        if (value != null) {
                            DBObject obj = marshall(value);
                            obj.removeField("_id");  //Do not store ID embedded!
                            v = obj;
                        }
                    } else if (annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Embedded.class)) {
                        if (value != null) {
                            v = marshall(value);
                        }
                    } else {
                        v = value;
                        if (v != null) {
                            if (v instanceof Map) {
                                //create MongoDBObject-Map
                                v = createDBMap((Map) v);
                            } else if (v instanceof List) {
                                v = createDBList((List) v);
                            } else if (v instanceof Iterable) {
                                ArrayList lst = new ArrayList();
                                for (Object i : (Iterable) v) {
                                    lst.add(i);
                                }
                                v = createDBList(lst);
                            } else if (v.getClass().equals(GregorianCalendar.class)) {
                                v = ((GregorianCalendar) v).getTime();
                            } else if (v.getClass().isEnum()) {
                                v = ((Enum) v).name();
                            }
                        }
                    }
                }
                if (v == null) {
                    if (!fld.isAnnotationPresent(UseIfnull.class)) {
                        //Do not put null-Values into dbo => not storing null-Values to db
                        continue;
                    }
                }
                dbo.put(fName, v);

            } catch (IllegalAccessException exc) {
                log.fatal("Illegal Access to field " + f);
            }

        }
        return dbo;
    }

    private BasicDBList createDBList(List v) {
        BasicDBList lst = new BasicDBList();
        for (Object lo : v) {
            if (lo != null) {
                if (annotationHelper.isAnnotationPresentInHierarchy(lo.getClass(), Entity.class) ||
                        annotationHelper.isAnnotationPresentInHierarchy(lo.getClass(), Embedded.class)) {
                    DBObject marshall = marshall(lo);
                    marshall.put("class_name", lo.getClass().getName());
                    lst.add(marshall);
                } else if (lo instanceof List) {
                    lst.add(createDBList((List) lo));
                } else if (lo instanceof Map) {
                    lst.add(createDBMap(((Map) lo)));
                } else if (lo.getClass().isEnum()) {
                    BasicDBObject obj = new BasicDBObject();
                    obj.put("class_name", lo.getClass().getName());
                    obj.put("name", ((Enum) lo).name());
                    lst.add(obj);
                    //throw new IllegalArgumentException("List of enums not supported yet");
                } else {
                    lst.add(lo);
                }
            } else {
                lst.add(null);
            }
        }
        return lst;
    }

    @SuppressWarnings("unchecked")
    private BasicDBObject createDBMap(Map v) {
        BasicDBObject dbMap = new BasicDBObject();
        for (Map.Entry<Object, Object> es : ((Map<Object, Object>) v).entrySet()) {
            Object k = es.getKey();
            if (!(k instanceof String)) {
                log.warn("Map in Mongodb needs to have String as keys - using toString");
                k = k.toString();
                if (((String) k).contains(".")) {
                    log.warn(". not allowed as Key in Maps - converting to _");
                    k = ((String) k).replaceAll("\\.", "_");
                }
            }
            Object mval = es.getValue(); // ((Map) v).get(k);
            if (mval != null) {
                if (annotationHelper.isAnnotationPresentInHierarchy(mval.getClass(), Entity.class) || annotationHelper.isAnnotationPresentInHierarchy(mval.getClass(), Embedded.class)) {
                    DBObject obj = marshall(mval);
                    obj.put("class_name", mval.getClass().getName());
                    mval = obj;
                } else if (mval instanceof Map) {
                    mval = createDBMap((Map) mval);
                } else if (mval instanceof List) {
                    mval = createDBList((List) mval);
                } else if (mval.getClass().isEnum()) {
                    BasicDBObject obj = new BasicDBObject();
                    obj.put("class_name", mval.getClass().getName());
                    obj.put("name", ((Enum) mval).name());
                }
            }
            dbMap.put((String) k, mval);
        }
        return dbMap;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T unmarshall(Class<? extends T> cls, DBObject o) {
        try {
            if (o.get("class_name") != null || o.get("className") != null) {
                if (log.isDebugEnabled()) {
                    log.debug("overriding cls - it's defined in dbObject");
                }
                try {
                    String cN = (String) o.get("class_name");
                    if (cN == null) {
                        cN = (String) o.get("className");
                    }
                    cls = (Class<? extends T>) Class.forName(cN);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            if (cls.isEnum()) {
                T[] en = cls.getEnumConstants();
                for (Enum e : ((Enum[]) en)) {
                    if (e.name().equals(o.get("name"))) {
                        return (T) e;
                    }
                }
            }

            T ret = cls.newInstance();

            List<String> flds = annotationHelper.getFields(cls);

            for (String f : flds) {
                Object valueFromDb = o.get(f);
                Field fld = annotationHelper.getField(cls, f);
                if (Modifier.isStatic(fld.getModifiers())) {
                    //skip static fields
                    continue;
                }


                if (fld.isAnnotationPresent(AdditionalData.class)) {
                    //this field should store all data that is not put to fields
                    if (!Map.class.isAssignableFrom(fld.getType())) {
                        log.error("Could not unmarshall additional data into fld of type " + fld.getType().toString());
                        continue;
                    }
                    Set<String> keys = o.keySet();
                    Map<String, Object> data = new HashMap<String, Object>();
                    for (String k : keys) {
                        if (flds.contains(k)) {
                            continue;
                        }
                        data.put(k, o.get(k));
                    }
                    fld.set(ret, data);
                    continue;
                }
                if (valueFromDb == null) {
                    continue;
                }
                Object value = null;
                if (!Map.class.isAssignableFrom(fld.getType()) && !Collection.class.isAssignableFrom(fld.getType()) && fld.isAnnotationPresent(Reference.class)) {
                    //A reference - only id stored
                    Reference reference = fld.getAnnotation(Reference.class);
                    if (morphium == null) {
                        log.fatal("Morphium not set - could not de-reference!");
                    } else {
                        ObjectId id = null;
                        if (valueFromDb instanceof ObjectId) {
                            id = (ObjectId) valueFromDb;
                        } else {
                            DBRef ref = (DBRef) valueFromDb;
                            if (ref != null) {
                                id = (ObjectId) ref.getId();
                                if (!ref.getRef().equals(fld.getType().getName())) {
                                    log.warn("Reference to different object?! - continuing anyway");

                                }
                            }
                        }
                        if (id != null) {
                            if (reference.lazyLoading()) {
                                List<String> lst = annotationHelper.getFields(fld.getType(), Id.class);
                                if (lst.size() == 0)
                                    throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                                value = morphium.createLazyLoadedEntity(fld.getType(), id);
                            } else {
//                                Query q = morphium.createQueryFor(fld.getSearchType());
//                                q.f("_id").eq(id);
                                value = morphium.findById(fld.getType(), id);
                            }
                        } else {
                            value = null;
                        }

                    }
                } else if (fld.isAnnotationPresent(Id.class)) {
                    value = o.get("_id");
                } else if (annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Entity.class) || annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Embedded.class)) {
                    //entity! embedded
                    value = unmarshall(fld.getType(), (DBObject) valueFromDb);
                } else if (Map.class.isAssignableFrom(fld.getType())) {
                    BasicDBObject map = (BasicDBObject) valueFromDb;
                    value = createMap(map);
                } else if (Collection.class.isAssignableFrom(fld.getType()) || fld.getType().isArray()) {
                    if (fld.getType().equals(byte[].class)) {
                        //binary data
                        if (log.isDebugEnabled())
                            log.debug("Reading in binary data object");
                        value = valueFromDb;

                    } else {
                        BasicDBList l = (BasicDBList) valueFromDb;
                        List lst = new ArrayList();
                        if (l != null) {
                            fillList(fld, l, lst);
                            if (fld.getType().isArray()) {
                                Object arr = Array.newInstance(fld.getType().getComponentType(), lst.size());
                                for (int i = 0; i < lst.size(); i++) {
                                    if (fld.getType().getComponentType().isPrimitive()) {
                                        if (fld.getType().getComponentType().equals(int.class)) {
                                            Array.set(arr, i, ((Integer) lst.get(i)).intValue());
                                        } else if (fld.getType().getComponentType().equals(long.class)) {
                                            Array.set(arr, i, ((Long) lst.get(i)).longValue());
                                        } else if (fld.getType().getComponentType().equals(float.class)) {
                                            //Driver sends doubles instead of floats
                                            Array.set(arr, i, ((Double) lst.get(i)).floatValue());

                                        } else if (fld.getType().getComponentType().equals(double.class)) {
                                            Array.set(arr, i, ((Double) lst.get(i)).doubleValue());

                                        } else if (fld.getType().getComponentType().equals(boolean.class)) {
                                            Array.set(arr, i, ((Boolean) lst.get(i)).booleanValue());

                                        }
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }
                                }
                                value = arr;
                            } else {
                                value = lst;
                            }
                        } else {
                            value = l;
                        }
                    }
                } else if (fld.getType().isEnum()) {
                    value = Enum.valueOf((Class<? extends Enum>) fld.getType(), (String) valueFromDb);
                } else {
                    value = valueFromDb;
                }
                annotationHelper.setValue(ret, value, f);
            }

            if (annotationHelper.isAnnotationPresentInHierarchy(cls, Entity.class)) {
                flds = annotationHelper.getFields(cls, Id.class);
                if (flds.isEmpty()) {
                    throw new RuntimeException("Error - class does not have an ID field!");
                }

                annotationHelper.getField(cls, flds.get(0)).set(ret, o.get("_id"));
            }
            if (annotationHelper.isAnnotationPresentInHierarchy(cls, PartialUpdate.class) || cls.isInstance(PartiallyUpdateable.class)) {
                return morphium.createPartiallyUpdateableEntity(ret);
            }
            return ret;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        //recursively fill class

    }

    private Object createMap(BasicDBObject map) {
        Object value;
        if (map != null) {
            for (String n : map.keySet()) {

                if (map.get(n) instanceof BasicDBObject) {
                    Object val = map.get(n);
                    if (((BasicDBObject) val).containsField("class_name") || ((BasicDBObject) val).containsField("className")) {
                        //Entity to map!
                        String cn = (String) ((BasicDBObject) val).get("class_name");
                        if (cn == null) {
                            cn = (String) ((BasicDBObject) val).get("className");
                        }
                        try {
                            Class ecls = Class.forName(cn);
                            map.put(n, unmarshall(ecls, (DBObject) map.get(n)));
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        //maybe a map of maps? --> recurse
                        map.put(n, createMap((BasicDBObject) val));
                    }
                } else if (map.get(n) instanceof BasicDBList) {
                    BasicDBList lst = (BasicDBList) map.get(n);
                    List mapValue = createList(lst);
                    map.put(n, mapValue);
                }
            }
            value = map;
        } else {
            value = null;
        }
        return value;
    }

    private List createList(BasicDBList lst) {
        List mapValue = new ArrayList();
        for (Object li : lst) {
            if (li instanceof BasicDBObject) {
                if (((BasicDBObject) li).containsField("class_name") || ((BasicDBObject) li).containsField("className")) {
                    String cn = (String) ((BasicDBObject) li).get("class_name");
                    if (cn == null) {
                        cn = (String) ((BasicDBObject) li).get("className");
                    }
                    try {
                        Class ecls = Class.forName(cn);
                        mapValue.add(unmarshall(ecls, (DBObject) li));
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    mapValue.add(createMap((BasicDBObject) li));
                }
            } else if (li instanceof BasicDBList) {
                mapValue.add(createList((BasicDBList) li));
            } else {
                mapValue.add(li);
            }
        }
        return mapValue;
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private void fillList(Field forField, BasicDBList fromDB, List toFillIn) {
        for (Object val : fromDB) {
            if (val instanceof BasicDBObject) {
                if (((BasicDBObject) val).containsField("class_name") || ((BasicDBObject) val).containsField("className")) {
                    //Entity to map!
                    String cn = (String) ((BasicDBObject) val).get("class_name");
                    if (cn == null) {
                        cn = (String) ((BasicDBObject) val).get("className");
                    }
                    try {
                        Class ecls = Class.forName(cn);
                        toFillIn.add(unmarshall(ecls, (DBObject) val));
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    //Probably an "normal" map
                    toFillIn.add(val);
                }
            } else if (val instanceof ObjectId) {
                log.warn("Cannot de-reference to unknown collection - trying to add ObjectId only");
                toFillIn.add(val);

            } else if (val instanceof BasicDBList) {
                //one list once more
                ArrayList lt = new ArrayList();
                fillList(null, (BasicDBList) val, lt);
                toFillIn.add(lt);
            } else if (val instanceof DBRef) {
                try {
                    DBRef ref = (DBRef) val;
                    ObjectId id = (ObjectId) ref.getId();
                    Class clz = Class.forName(ref.getRef());
                    List<String> idFlds = annotationHelper.getFields(clz, Id.class);
                    Reference reference = forField != null ? forField.getAnnotation(Reference.class) : null;

                    if (reference != null && reference.lazyLoading()) {
                        if (idFlds.size() == 0)
                            throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                        toFillIn.add(morphium.createLazyLoadedEntity(clz, id));
                    } else {
                        Query q = morphium.createQueryFor(clz);
                        q = q.f(idFlds.get(0)).eq(id);
                        toFillIn.add(q.get());
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }

            } else {
                toFillIn.add(val);
            }
        }
    }

}
