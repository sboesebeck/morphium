package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import de.caluga.morphium.annotations.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 19:36
 * <p/>
 */
public class ObjectMapperImpl implements ObjectMapper {
    private static Logger log = Logger.getLogger(ObjectMapperImpl.class);
    private static volatile Map<Class<?>, List<Field>> fieldCache = new Hashtable<Class<?>, List<Field>>();
    public volatile Morphium morphium;
    private volatile Hashtable<Class<?>, NameProvider> nameProviders;

    public Morphium getMorphium() {
        return morphium;
    }

    public void setMorphium(Morphium morphium) {
        this.morphium = morphium;
    }

    public ObjectMapperImpl(Morphium m) {
        morphium = m;
        nameProviders = new Hashtable<Class<?>, NameProvider>();
    }

    public ObjectMapperImpl() {
        this(null);
    }

    /**
     * converts a sql/javascript-Name to Java, e.g. converts document_id to
     * documentId.
     *
     * @param n
     * @param capitalize : if true, first letter will be capitalized
     * @return
     */
    public String createCamelCase(String n, boolean capitalize) {
        n = n.toLowerCase();
        String f[] = n.split("_");
        StringBuilder sb = new StringBuilder(f[0].substring(0, 1).toLowerCase());
        //String ret =
        sb.append(f[0].substring(1));
        for (int i = 1; i < f.length; i++) {
            sb.append(f[i].substring(0, 1).toUpperCase());
            sb.append(f[i].substring(1));
        }
        String ret = sb.toString();
        if (capitalize) {
            ret = ret.substring(0, 1).toUpperCase() + ret.substring(1);
        }
        return ret;
    }

    /**
     * turns documentId into document_id
     *
     * @param n
     * @return
     */
    public String convertCamelCase(String n) {
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < n.length() - 1; i++) {
            if (Character.isUpperCase(n.charAt(i)) && i > 0) {
                b.append("_");
            }
            b.append(n.substring(i, i + 1).toLowerCase());
        }
        b.append(n.substring(n.length() - 1));
        return b.toString();
    }

    /**
     * override nameprovider in runtime!
     *
     * @param cls
     * @param np
     */
    public void setNameProviderForClass(Class<?> cls, NameProvider np) {
        nameProviders.put(cls, np);
    }

    public NameProvider getNameProviderForClass(Class<?> cls) {
        Entity e = morphium.getAnnotationFromHierarchy(cls, Entity.class);
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

    @Override
    public String getCollectionName(Class cls) {
        if (morphium == null) return null;
        Entity p = morphium.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) cls.getAnnotation(Entity.class);
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }
        try {
            cls = getRealClass(cls);
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

    @Override
    public DBObject marshall(Object o) {
        //recursively map object ot mongo-Object...
        if (!isEntity(o)) {
            throw new IllegalArgumentException("Object is no entity: " + o.getClass().getSimpleName());
        }

        DBObject dbo = new BasicDBObject();
        if (o == null) {
            return dbo;
        }
        Class<?> cls = getRealClass(o.getClass());
        if (cls == null) {
            throw new IllegalArgumentException("No real class?");
        }
        o = getRealObject(o);
        List<String> flds = getFields(cls);
        if (flds == null) {
            throw new IllegalArgumentException("Fields not found? " + cls.getName());
        }
        Entity e = morphium.getAnnotationFromHierarchy(o.getClass(), Entity.class); //o.getClass().getAnnotation(Entity.class);
        Embedded emb = morphium.getAnnotationFromHierarchy(o.getClass(), Embedded.class); //o.getClass().getAnnotation(Embedded.class);

        if (e != null && e.polymorph()) {
            dbo.put("class_name", cls.getName());
        }

        if (emb != null && emb.polymorph()) {
            dbo.put("class_name", cls.getName());
        }

        for (String f : flds) {
            String fName = f;
            try {
                Field fld = getField(cls, f);
                if (fld == null) {
                    log.error("Field not found");
                    continue;
                }
                //do not store static fields!
                if (Modifier.isStatic(fld.getModifiers())) {
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
                        if (fld.getType().isAssignableFrom(List.class)) {
                            //list of references....
                            BasicDBList lst = new BasicDBList();
                            for (Object rec : ((List) value)) {
                                if (rec != null) {
                                    ObjectId id = getId(rec);
                                    if (id == null) {
                                        if (r.automaticStore()) {
                                            morphium.storeNoCache(rec);
                                            id = getId(rec);
                                        } else {
                                            throw new IllegalArgumentException("Cannot store reference to unstored entity if automaticStore in @Reference is set to false!");
                                        }
                                    }
                                    DBRef ref = new DBRef(morphium.getDatabase(), getRealClass(rec.getClass()).getName(), id);
                                    lst.add(ref);
                                } else {
                                    lst.add(null);
                                }
                            }
                            v = lst;
                        } else if (fld.getType().isAssignableFrom(Map.class)) {
                            throw new RuntimeException("Cannot store references in Maps!");
                        } else {

                            if (getId(value) == null) {
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
                            v = getId(value);
                        }
                    }
                } else {

                    //check, what type field has

                    //Store Entities recursively
                    //TODO: Fix recursion - this could cause a loop!
                    if (morphium.isAnnotationPresentInHierarchy(fld.getType(), Entity.class)) {
                        if (value != null) {
                            DBObject obj = marshall(value);
                            obj.removeField("_id");  //Do not store ID embedded!
                            v = obj;
                        }
                    } else if (morphium.isAnnotationPresentInHierarchy(fld.getType(), Embedded.class)) {
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
                            } else if (v.getClass().isEnum()) {
                                v = ((Enum) v).name();
                            }
                        }
                    }
                }
                if (v == null) {
                    if (fld.isAnnotationPresent(NotNull.class)) {
                        throw new IllegalArgumentException("Value is null - but must not (NotNull-Annotation to" + o.getClass().getSimpleName() + ")! Field: " + fName);
                    }
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
        for (Object lo : ((List) v)) {
            if (lo != null) {
                if (morphium.isAnnotationPresentInHierarchy(lo.getClass(), Entity.class) ||
                        morphium.isAnnotationPresentInHierarchy(lo.getClass(), Embedded.class)) {
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
                if (morphium.isAnnotationPresentInHierarchy(mval.getClass(), Entity.class) || morphium.isAnnotationPresentInHierarchy(mval.getClass(), Embedded.class)) {
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


    @Override
    public <T> T unmarshall(Class<T> cls, DBObject o) {
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
                    cls = (Class<T>) Class.forName(cN);
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

            List<String> flds = getFields(cls);

            for (String f : flds) {
                Field fld = getField(cls, f);
                if (Modifier.isStatic(fld.getModifiers())) {
                    //skip static fields
                    continue;
                }

                Object value = null;
                if (!fld.getType().isAssignableFrom(Map.class) && !fld.getType().isAssignableFrom(List.class) && fld.isAnnotationPresent(Reference.class)) {
                    //A reference - only id stored
                    Reference reference = fld.getAnnotation(Reference.class);
                    if (morphium == null) {
                        log.fatal("Morphium not set - could not de-reference!");
                    } else {
                        ObjectId id = null;
                        if (o.get(f) instanceof ObjectId) {
                            id = (ObjectId) o.get(f);
                        } else {
                            DBRef ref = (DBRef) o.get(f);
                            if (ref != null) {
                                id = (ObjectId) ref.getId();
                                if (!ref.getRef().equals(fld.getType().getName())) {
                                    log.warn("Reference to different object?! - continuing anyway");

                                }
                            }
                        }
                        if (id != null) {
                            if (reference.lazyLoading()) {
                                List<String> lst = getFields(fld.getType(), Id.class);
                                if (lst.size() == 0)
                                    throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                                value = morphium.createLazyLoadedEntity(fld.getType(), id);
                            } else {
//                                Query q = morphium.createQueryFor(fld.getType());
//                                q.f("_id").eq(id);
                                value = morphium.findById(fld.getType(), id);
                            }
                        } else {
                            value = null;
                        }

                    }
                } else if (fld.isAnnotationPresent(Id.class)) {
                    value = (ObjectId) o.get("_id");
                } else if (morphium.isAnnotationPresentInHierarchy(fld.getType(), Entity.class) || morphium.isAnnotationPresentInHierarchy(fld.getType(), Embedded.class)) {
                    //entity! embedded
                    if (o.get(f) != null) {
                        value = unmarshall(fld.getType(), (DBObject) o.get(f));
                    } else {
                        value = null;
                    }
                } else if (fld.getType().isAssignableFrom(Map.class)) {
                    BasicDBObject map = (BasicDBObject) o.get(f);
                    value = createMap(map);
                } else if (fld.getType().isAssignableFrom(List.class) || fld.getType().isArray()) {
                    BasicDBList l = (BasicDBList) o.get(f);
                    List lst = new ArrayList();
                    if (l != null) {
                        fillList(fld, l, lst);
                        if (fld.getType().isArray()) {
                            Object arr = Array.newInstance(fld.getType().getComponentType(), lst.size());
                            for (int i = 0; i < lst.size(); i++) {
                                Array.set(arr, i, lst.get(i));
                            }
                            value = arr;
                        } else {
                            value = lst;
                        }
                    } else {
                        value = l;
                    }
                } else if (fld.getType().isEnum()) {
                    if (o.get(f) != null) {
                        value = Enum.valueOf((Class<? extends Enum>) fld.getType(), (String) o.get(f));
                    }
                } else {
                    value = o.get(f);
                }
                setValue(ret, value, f);
            }

            if (morphium.isAnnotationPresentInHierarchy(cls, Entity.class)) {
                flds = getFields(cls, Id.class);
                if (flds.isEmpty()) {
                    throw new RuntimeException("Error - class does not have an ID field!");
                }

                getField(cls, flds.get(0)).set(ret, o.get("_id"));
            }
            if (morphium.isAnnotationPresentInHierarchy(cls, PartialUpdate.class) || cls.isInstance(PartiallyUpdateable.class)) {
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
                log.fatal("Cannot de-reference to unknown collection");

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
                    List<String> idFlds = getFields(clz, Id.class);
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

    @Override
    public ObjectId getId(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("Object cannot be null");
        }
        Class<?> cls = getRealClass(o.getClass());
        List<String> flds = getFields(cls, Id.class);
        if (flds == null || flds.isEmpty()) {
            throw new IllegalArgumentException("Object has no id defined: " + o.getClass().getSimpleName());
        }
        Field f = getField(cls, flds.get(0)); //first Id
        if (f == null) {
            throw new IllegalArgumentException("Object ID field not found " + o.getClass().getSimpleName());
        }
        try {
            if (!(f.getType().equals(ObjectId.class))) {
                throw new IllegalArgumentException("ID sould be of type ObjectId");
            }
            o = getRealObject(o);
            if (o != null) {
                return (ObjectId) f.get(o);
            } else {
                log.warn("Illegal reference?");
            }

            return null;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * return list of fields in class - including hierachy!!!
     *
     * @param clz
     * @return
     */
    public List<Field> getAllFields(Class clz) {
        Class<?> cls = getRealClass(clz);
        if (fieldCache.containsKey(cls)) {
            return fieldCache.get(cls);
        }
        List<Field> ret = new Vector<Field>();
        Class sc = cls;
        //getting class hierachy
        List<Class> hierachy = new Vector<Class>();
        while (!sc.equals(Object.class)) {
            hierachy.add(sc);
            sc = sc.getSuperclass();
        }
        for (Class c : cls.getInterfaces()) {
            hierachy.add(c);
        }
        //now we have a list of all classed up to Object
        //we need to run through it in the right order
        //in order to allow Inheritance to "shadow" fields
        for (int i = hierachy.size() - 1; i >= 0; i--) {
            Class c = hierachy.get(i);
            for (Field f : c.getDeclaredFields()) {
                ret.add(f);
            }
        }

        fieldCache.put(cls, ret);
        return ret;
    }

    @Override
    /**
     * get a list of valid fields of a given record as they are in the MongoDB
     * so, if you have a field Mapping, the mapped Property-name will be used
     * returns all fields, which have at least one of the given annotations
     * if no annotation is given, all fields are returned
     * Does not take the @Aliases-annotation int account
     *
     * @param cls
     * @return
     */
    public List<String> getFields(Class cls, Class<? extends Annotation>... annotations) {
        List<String> ret = new Vector<String>();
        Class sc = cls;
        sc = getRealClass(sc);
        Entity entity = morphium.getAnnotationFromHierarchy(sc, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
        Embedded embedded = morphium.getAnnotationFromHierarchy(sc, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);
        if (embedded != null && entity != null) {
            log.warn("Class " + cls.getName() + " does have both @Entity and @Embedded Annotations - not allowed! Assuming @Entity is right");
        }

        if (embedded == null && entity == null) {
            throw new IllegalArgumentException("This class " + cls.getName() + " does not have @Entity or @Embedded set, not even in hierachy - illegal!");
        }
        boolean tcc = entity == null ? embedded.translateCamelCase() : entity.translateCamelCase();
        //getting class hierachy
        List<Field> fld = getAllFields(cls);
        for (Field f : fld) {
            if (annotations.length > 0) {
                boolean found = false;
                for (Class<? extends Annotation> a : annotations) {
                    if (f.isAnnotationPresent(a)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    //no annotation found
                    continue;
                }
            }
            if (f.isAnnotationPresent(Reference.class) && !".".equals(f.getAnnotation(Reference.class).fieldName())) {
                ret.add(f.getAnnotation(Reference.class).fieldName());
                continue;
            }
            if (f.isAnnotationPresent(Property.class) && !".".equals(f.getAnnotation(Property.class).fieldName())) {
                ret.add(f.getAnnotation(Property.class).fieldName());
                continue;
            }
//            if (f.isAnnotationPresent(Id.class)) {
//                ret.add(f.getName());
//                continue;
//            }
            if (f.isAnnotationPresent(Transient.class)) {
                continue;
            }

            if (tcc) {
                ret.add(convertCamelCase(f.getName()));
            } else {
                ret.add(f.getName());
            }
        }

        return ret;
    }

    @Override
    public <T> Class<T> getRealClass(Class<T> sc) {
        if (sc.getName().contains("$$EnhancerByCGLIB$$")) {

            try {
                sc = (Class<T>) Class.forName(sc.getName().substring(0, sc.getName().indexOf("$$")));
            } catch (Exception e) {
                //TODO: Implement Handling
                throw new RuntimeException(e);
            }
        }
        return sc;
    }

    @Override
    public String getFieldName(Class clz, String field) {
        Class cls = getRealClass(clz);
        if (field.contains(".")) {
            //searching for a sub-element?
            //no check possible
            return field;
        }
        Field f = getField(cls, field);
        if (f == null) throw new RuntimeException("Field not found " + field + " in cls: " + clz.getName());
        if (f.isAnnotationPresent(Property.class)) {
            Property p = f.getAnnotation(Property.class);
            if (p.fieldName() != null && !p.fieldName().equals(".")) {
                return p.fieldName();
            }
        }

        if (f.isAnnotationPresent(Reference.class)) {
            Reference p = f.getAnnotation(Reference.class);
            if (p.fieldName() != null && !p.fieldName().equals(".")) {
                return p.fieldName();
            }
        }
        if (f.isAnnotationPresent(Id.class)) {
            return "_id";
        }


        String fieldName = f.getName();
        Entity ent = morphium.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) cls.getAnnotation(Entity.class);
        Embedded emb = morphium.getAnnotationFromHierarchy(cls, Embedded.class);//(Embedded) cls.getAnnotation(Embedded.class);
        if (ent != null && ent.translateCamelCase()) {
            fieldName = convertCamelCase(fieldName);
        } else if (emb != null && emb.translateCamelCase()) {
            fieldName = convertCamelCase(fieldName);
        }

        return fieldName;

    }

    @Override
    /**
     * extended logic: Fld may be, the java field name, the name of the specified value in Property-Annotation or
     * the translated underscored lowercase name (mongoId => mongo_id) or a name specified in the Aliases-Annotation of this field
     *
     * @param clz - class to search
     * @param fld - field name
     * @return field, if found, null else
     */
    public Field getField(Class clz, String fld) {
        Class cls = getRealClass(clz);
        List<Field> flds = getAllFields(cls);
        for (Field f : flds) {
            if (f.isAnnotationPresent(Property.class) && f.getAnnotation(Property.class).fieldName() != null && !".".equals(f.getAnnotation(Property.class).fieldName())) {
                if (f.getAnnotation(Property.class).fieldName().equals(fld)) {
                    f.setAccessible(true);
                    return f;
                }
            }
            if (f.isAnnotationPresent(Reference.class) && f.getAnnotation(Reference.class).fieldName() != null && !".".equals(f.getAnnotation(Reference.class).fieldName())) {
                if (f.getAnnotation(Reference.class).fieldName().equals(fld)) {
                    f.setAccessible(true);
                    return f;
                }
            }
            if (f.isAnnotationPresent(Aliases.class)) {
                Aliases aliases = f.getAnnotation(Aliases.class);
                String[] v = aliases.value();
                for (String field : v) {
                    if (field.equals(fld)) {
                        f.setAccessible(true);
                        return f;
                    }
                }
            }
            if (fld.equals("_id")) {
                if (f.isAnnotationPresent(Id.class)) {
                    f.setAccessible(true);
                    return f;
                }
            }
            if (f.getName().equals(fld)) {
                f.setAccessible(true);
                return f;
            }
            if (convertCamelCase(f.getName()).equals(fld)) {
                f.setAccessible(true);
                return f;
            }


        }

        return null;
    }


    @Override
    public boolean isEntity(Object o) {
        Class cls = null;
        if (o == null) return false;

        if (o instanceof Class) {
            cls = getRealClass((Class) o);
        } else {
            cls = getRealClass(o.getClass());
        }
        return morphium.isAnnotationPresentInHierarchy(cls, Entity.class) || morphium.isAnnotationPresentInHierarchy(cls, Embedded.class);
    }

    @Override
    public Object getValue(Object o, String fld) {
        if (o == null) {
            return null;
        }
        try {
            Field f = getField(o.getClass(), fld);
            if (!Modifier.isStatic(f.getModifiers())) {
                o = getRealObject(o);
                return f.get(o);
            }
        } catch (IllegalAccessException e) {
            log.fatal("Illegal access to field " + fld + " of type " + o.getClass().getSimpleName());

        }
        return null;
    }

    @Override
    public void setValue(Object o, Object value, String fld) {
        if (o == null) {
            return;
        }
        try {
            Field f = getField(getRealClass(o.getClass()), fld);
            if (!Modifier.isStatic(f.getModifiers())) {
                o = getRealObject(o);
                try {
                    f.set(o, value);
                } catch (Exception e) {
                    if (value == null) {
                        try {
                            //try to set 0 instead
                            f.set(o, 0);
                        } catch (Exception e1) {
                            //Still not working? Maybe boolean?
                            f.set(o, false);
                        }
                    } else {
                        throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                    }

                }
            }
        } catch (IllegalAccessException e) {
            log.fatal("Illegal access to field " + fld + " of toype " + o.getClass().getSimpleName());
            return;
        }
    }

    @Override
    public <T> T getRealObject(T o) {
        if (o.getClass().getName().contains("$$EnhancerByCGLIB$$")) {
            //not stored or Proxy?
            try {
                Field f1 = o.getClass().getDeclaredField("CGLIB$CALLBACK_0");
                f1.setAccessible(true);
                Object delegate = f1.get(o);
                Method m = delegate.getClass().getMethod("__getDeref");
                o = (T) m.invoke(delegate);
            } catch (Exception e) {
                //throw new RuntimeException(e);
                log.error("Exception: ", e);
            }
        }
        return o;
    }
}
