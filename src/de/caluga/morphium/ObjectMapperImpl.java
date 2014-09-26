package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import sun.reflect.ReflectionFactory;

import java.lang.reflect.*;
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
    private volatile AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper(true);
    private Morphium morphium;
    private JSONParser jsonParser = new JSONParser();
    private EntityCache cache;

    public ObjectMapperImpl() {
        cache = new EntityCache();

    }

    @Override
    public Class<?> getClassforTypeId(String id) {
        return null;
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
            annotationHelper = new AnnotationAndReflectionHelper(true);
        }
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    /**
     * override nameprovider in runtime!
     *
     * @param cls - class to use
     * @param np  - the NameProvider for that class
     */
    public void setNameProviderForClass(Class<?> cls, NameProvider np) {
        cache.getNameProviders().put(cls, np);
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

        if (cache.getNameProviders().get(cls) == null) {
            NameProvider np = p.nameProvider().newInstance();
            cache.getNameProviders().put(cls, np);
        }
        return cache.getNameProviders().get(cls);
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
            if (e.typeId().equals(".")) {
                dbo.put("type_id", cls.getName());
            } else {
                dbo.put("type_id", e.typeId());
            }
        }
        if (emb != null && emb.polymorph()) {
            if (emb.typeId().equals(".")) {
                dbo.put("type_id", cls.getName());
            } else {
                dbo.put("type_id", emb.typeId());
            }
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
                if (fld.isAnnotationPresent(ReadOnly.class)) {
                    continue; //do not write value
                }
                AdditionalData ad = fld.getAnnotation(AdditionalData.class);
                if (ad != null) {
                    if (!ad.readOnly()) {
                        //storing additional data
                        if (fld.get(o) != null) {
                            dbo.putAll((Map) createDBMap((Map<String, Object>) fld.get(o)));
                        }
                    }
                    //additional data is usually transient
                    continue;
                }
                if (dbo.containsField(fName)) {
                    //already stored, skip it
                    log.warn("Field " + fName + " is shadowed - inherited values?");
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
                                    Object id = annotationHelper.getId(rec);
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
                Entity ent = annotationHelper.getAnnotationFromHierarchy(lo.getClass(), Entity.class);
                Embedded emb = annotationHelper.getAnnotationFromHierarchy(lo.getClass(), Embedded.class);
                if (ent != null || emb != null) {
                    DBObject marshall = marshall(lo);
                    if (ent != null) {
                        if (ent.typeId().equals(".")) {
                            marshall.put("type_id", lo.getClass().getName());
                        } else {
                            marshall.put("type_id", ent.typeId());
                        }

                    }
                    if (emb != null) {
                        if (emb.typeId().equals(".")) {
                            marshall.put("type_id", lo.getClass().getName());
                        } else {
                            marshall.put("type_id", emb.typeId());
                        }

                    }
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
                Entity ent = annotationHelper.getAnnotationFromHierarchy(mval.getClass(), Entity.class);
                Embedded emb = annotationHelper.getAnnotationFromHierarchy(mval.getClass(), Embedded.class);
                if (ent != null || emb != null) {
                    DBObject obj = marshall(mval);
                    if (ent != null) {
                        if (ent.typeId().equals(".")) {
                            obj.put("type_id", mval.getClass().getName());
                        } else {
                            obj.put("type_id", ent.typeId());
                        }
                    } else if (emb != null) {
                        if (emb.typeId().equals(".")) {
                            obj.put("type_id", mval.getClass().getName());
                        } else {
                            obj.put("type_id", ent.typeId());
                        }
                    }
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

    @Override
    public <T> T unmarshall(Class<? extends T> cls, String jsonString) throws ParseException {
        ContainerFactory fact = new ContainerFactory() {
            @Override
            public Map createObjectContainer() {
                return new BasicDBObject();
            }

            @Override
            public List creatArrayContainer() {
                return new BasicDBList();
            }
        };
        DBObject obj = (DBObject) jsonParser.parse(jsonString, fact);
        return unmarshall(cls, obj);


    }

    public <T> T getEnum(Class<? extends T> cls, String name) {

        T[] en = cls.getEnumConstants();
        for (Enum e : ((Enum[]) en)) {
            if (e.name().equals(name)) {
                return (T) e;
            }
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T unmarshall(Class<? extends T> cls, DBObject o) {
        try {
            if (o.get("type_id") != null) {
                cls = cache.getEntityByTypeId().get(o.get("type_id"));
            } else if (o.get("class_name") != null || o.get("className") != null) {
//                if (log.isDebugEnabled()) {
//                    log.debug("overriding cls - it's defined in dbObject");
//                }
                String cN = (String) o.get("class_name");
                if (cN == null) {
                    cN = (String) o.get("className");
                }
                try {
                    cls = (Class<? extends T>) Class.forName(cN);
                } catch (ClassNotFoundException e) {

                    log.warn("could not load class " + cN);
                    if (o.get("name") != null) {
                        String name = (String) o.get("name");
                        log.warn(" trying recovery for Enums");
                        for (Class enumCls : cache.getEnumlist()) {
                            T en = (T) getEnum(enumCls, name);
                            if (en != null) {
                                log.warn(" => Using enum of type " + en.getClass().getName());
                                return en;
                            }
                        }

                    }
                    throw new RuntimeException(e);
                }
            }
            if (cls.isEnum()) {
                return getEnum(cls, (String) o.get("name"));
            }

            T ret = null;

            try {
                ret = cls.newInstance();
            } catch (Exception e) {
            }
            if (ret == null) {
                final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();
                final Constructor<Object> constructor;
                try {
                    constructor = reflection.newConstructorForSerialization(
                            cls, Object.class.getDeclaredConstructor());
                    ret = (T) constructor.newInstance();
                } catch (Exception e) {
                    log.error(e);
                }
            }
            if (ret == null) {
                throw new IllegalArgumentException("Could not instanciate " + cls.getName());
            }
            List<String> flds = annotationHelper.getFields(cls);

            for (String f : flds) {

                Object valueFromDb = o.get(f);
                Field fld = annotationHelper.getField(cls, f);
                if (Modifier.isStatic(fld.getModifiers())) {
                    //skip static fields
                    continue;
                }

                if (fld.isAnnotationPresent(WriteOnly.class)) {
                    continue;//do not read from DB
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
                        if (k.equals("_id")) {
                            //id already mapped
                            continue;
                        }

                        if (o.get(k) instanceof BasicDBObject) {
                            if (((BasicDBObject) o.get(k)).get("class_name") != null) {
                                data.put(k, unmarshall(Class.forName((String) ((BasicDBObject) o.get(k)).get("class_name")), (BasicDBObject) o.get(k)));
                            } else {
                                data.put(k, createMap((BasicDBObject) o.get(k)));
                            }
                        } else if (o.get(k) instanceof BasicDBList) {
                            data.put(k, createList((BasicDBList) o.get(k)));
                        } else {
                            data.put(k, o.get(k));
                        }

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
                        Object id = null;
                        if (valueFromDb instanceof Object) {
                            id = (Object) valueFromDb;
                        } else {
                            DBRef ref = (DBRef) valueFromDb;
                            if (ref != null) {
                                id = (Object) ref.getId();
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
                    if (!value.getClass().equals(fld.getType())) {
                        log.warn("read value and field type differ...");
                        if (fld.getType().equals(ObjectId.class)) {
                            log.warn("trying objectID conversion");
                            if (value.getClass().equals(String.class)) {
                                try {
                                    value = new ObjectId((String) value);
                                } catch (Exception e) {
                                    log.error("Id conversion failed - setting returning null", e);
                                    return null;
                                }
                            }
                        } else if (value.getClass().equals(ObjectId.class)) {
                            if (fld.getType().equals(String.class)) {
                                value = value.toString();
                            } else if (fld.getType().equals(Long.class) || fld.getType().equals(long.class)) {
                                value = ((ObjectId) value).getTime();
                            } else {
                                log.error("cannot convert - ID IS SET TO NULL. Type read from db is " + value.getClass().getName() + " - expected value is " + fld.getType().getName());
                                return null;
                            }
                        }
                    }
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

                        List lst = new ArrayList();
                        if (valueFromDb.getClass().isArray()) {
                            //a real array!
                            if (valueFromDb.getClass().getComponentType().isPrimitive()) {
                                if (valueFromDb.getClass().getComponentType().equals(int.class)) {
                                    for (int i : (int[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                } else if (valueFromDb.getClass().getComponentType().equals(double.class)) {
                                    for (double i : (double[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                } else if (valueFromDb.getClass().getComponentType().equals(float.class)) {
                                    for (float i : (float[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                } else if (valueFromDb.getClass().getComponentType().equals(boolean.class)) {
                                    for (boolean i : (boolean[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                } else if (valueFromDb.getClass().getComponentType().equals(byte.class)) {
                                    for (byte i : (byte[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                } else if (valueFromDb.getClass().getComponentType().equals(char.class)) {
                                    for (char i : (char[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                } else if (valueFromDb.getClass().getComponentType().equals(long.class)) {
                                    for (long i : (long[]) valueFromDb) {
                                        lst.add(i);
                                    }
                                }
                            } else {
                                for (Object vdb : (Object[]) valueFromDb) {
                                    lst.add(vdb);
                                }
                            }
                            value = lst;
                        } else {
                            BasicDBList l = (BasicDBList) valueFromDb;
                            if (l != null) {
                                fillList(fld, l, lst);
                            } else {
                                value = l;
                            }
                        }
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
                Field field = annotationHelper.getField(cls, flds.get(0));
                if (o.get("_id") != null) {  //Embedded entitiy?
                    if (o.get("_id").getClass().equals(field.getType())) {
                        field.set(ret, o.get("_id"));
                    } else if (field.getType().equals(String.class) && o.get("_id").getClass().equals(ObjectId.class)) {
                        log.warn("ID type missmatch - field is string but got objectId from mongo - converting");
                        field.set(ret, o.get("_id").toString());
                    } else if (field.getType().equals(ObjectId.class) && o.get("_id").getClass().equals(String.class)) {
                        log.warn("ID type missmatch - field is objectId but got string from db - trying conversion");
                        field.set(ret, new ObjectId((String) o.get("_id")));
                    } else {
                        log.error("ID type missmatch");
                        throw new IllegalArgumentException("ID type missmatch. Field in '" + ret.getClass().toString() + "' is '" + field.getType().toString() + "' but we got '" + o.get("_id").getClass().toString() + "' from Mongo!");
                    }
                }
            }
            if (annotationHelper.isAnnotationPresentInHierarchy(cls, PartialUpdate.class) || cls.isInstance(PartiallyUpdateable.class)) {
                return morphium.createPartiallyUpdateableEntity(ret);
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //recursively fill class

    }

    private Map createMap(BasicDBObject dbObject) {
        Map retMap = new HashMap(dbObject);
        if (dbObject != null) {
            for (String n : dbObject.keySet()) {

                if (dbObject.get(n) instanceof BasicDBObject) {
                    Object val = dbObject.get(n);

                    BasicDBObject dbo = (BasicDBObject) val;
                    if (dbo.containsField("type_id")) {
                        Class ecls = cache.getEntityByTypeId().get(dbo.get("type_id"));
                        Object obj = unmarshall(ecls, (DBObject) dbObject.get(n));
                        if (obj != null) retMap.put(n, obj);
                    } else if (dbo.containsField("class_name") || dbo.containsField("className")) {
                        //Entity to map!
                        String cn = (String) dbo.get("class_name");
                        if (cn == null) {
                            cn = (String) dbo.get("className");
                        }
                        try {
                            Class ecls = Class.forName(cn);
                            Object obj = unmarshall(ecls, (DBObject) dbObject.get(n));
                            if (obj != null) retMap.put(n, obj);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        //maybe a map of maps? --> recurse
                        retMap.put(n, createMap(dbo));
                    }
                } else if (dbObject.get(n) instanceof BasicDBList) {
                    BasicDBList lst = (BasicDBList) dbObject.get(n);
                    List mapValue = createList(lst);
                    retMap.put(n, mapValue);
                }
            }
        } else {
            retMap = null;
        }
        return retMap;
    }

    private List createList(BasicDBList lst) {
        List mapValue = new ArrayList();
        for (Object li : lst) {
            if (li instanceof BasicDBObject) {
                BasicDBObject bobj = (BasicDBObject) li;
                if (bobj.containsField("type_id")) {
                    Class cls = cache.getEntityByTypeId().get(bobj.get("type_id"));
                    Object obj = unmarshall(cls, bobj);
                    if (obj != null) mapValue.add(obj);
                } else if (bobj.containsField("class_name") || bobj.containsField("className")) {
                    String cn = (String) bobj.get("class_name");
                    if (cn == null) {
                        cn = (String) bobj.get("className");
                    }
                    try {
                        Class ecls = Class.forName(cn);
                        Object obj = unmarshall(ecls, bobj);
                        if (obj != null) mapValue.add(obj);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {


                    mapValue.add(createMap(bobj));
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
                if (((BasicDBObject) val).containsField("type_id")) {
                    //Entity to map!
                    Class ecls = cache.getEntityByTypeId().get(((BasicDBObject) val).get("type_id"));
                    if (ecls != null) {
                        Object um = unmarshall(ecls, (DBObject) val);
                        if (um != null) toFillIn.add(um);
                    } else throw new RuntimeException("Type_id unknown");
                } else if (((BasicDBObject) val).containsField("class_name") || ((BasicDBObject) val).containsField("className")) {
                    //Entity to map!
                    String cn = (String) ((BasicDBObject) val).get("class_name");
                    if (cn == null) {
                        cn = (String) ((BasicDBObject) val).get("className");
                    }
                    try {
                        Class ecls = Class.forName(cn);
                        Object um = unmarshall(ecls, (DBObject) val);
                        if (um != null) toFillIn.add(um);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {

                    if (forField != null && forField.getGenericType() instanceof ParameterizedType) {
                        //have a list of something
                        ParameterizedType listType = (ParameterizedType) forField.getGenericType();
                        while (listType.getActualTypeArguments()[0] instanceof ParameterizedType) {
                            listType = (ParameterizedType) listType.getActualTypeArguments()[0];
                        }
                        Class cls = (Class) listType.getActualTypeArguments()[0];
                        Entity entity = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
                        Embedded embedded = annotationHelper.getAnnotationFromHierarchy(cls, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);
                        if (entity != null || embedded != null)
                            val = unmarshall(cls, (DBObject) val);
                    }
                    //Probably an "normal" map
                    toFillIn.add(val);
                }
            } else if (val instanceof ObjectId) {
                if (forField.getGenericType() instanceof ParameterizedType) {
                    //have a list of something
                    ParameterizedType listType = (ParameterizedType) forField.getGenericType();
                    Class cls = (Class) listType.getActualTypeArguments()[0];
                    Query q = morphium.createQueryFor(cls);
                    q = q.f(annotationHelper.getFields(cls, Id.class).get(0)).eq(val);
                    toFillIn.add(q.get());
                } else {
                    log.warn("Cannot de-reference to unknown collection - trying to add Object only");
                    toFillIn.add(val);
                }

            } else if (val instanceof BasicDBList) {
                //list in list
                ArrayList lt = new ArrayList();
                fillList(forField, (BasicDBList) val, lt);
                toFillIn.add(lt);
            } else if (val instanceof DBRef) {
                try {
                    DBRef ref = (DBRef) val;
                    Object id = (Object) ref.getId();
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
