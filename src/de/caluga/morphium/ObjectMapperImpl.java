package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import de.caluga.morphium.annotations.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
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

    public Morphium getMorphium() {
        return morphium;
    }

    public void setMorphium(Morphium morphium) {
        this.morphium = morphium;
    }

    public ObjectMapperImpl(Morphium m) {
        morphium = m;
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
        String ret = f[0].substring(0, 1).toLowerCase() + f[0].substring(1);
        for (int i = 1; i < f.length; i++) {
            ret = ret + f[i].substring(0, 1).toUpperCase() + f[i].substring(1);
        }
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


    @Override
    public String getCollectionName(Class cls) {
        if (!cls.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }
        String name = cls.getSimpleName();

        Entity p = (Entity) cls.getAnnotation(Entity.class);
        if (p.useFQN()) {
            name = cls.getName().replaceAll("\\.", "_");
        }
        if (!p.collectionName().equals(".")) {
            name = p.collectionName();
        } else {
            if (p.translateCamelCase()) {
                name = convertCamelCase(name);
            }
        }
        return name;
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
        List<String> flds = getFields(o.getClass());
        Entity e = o.getClass().getAnnotation(Entity.class);
        if (e.polymorph()) {
            dbo.put("class_name", o.getClass().getName());
        }
        for (String f : flds) {
            String fName = f;
            try {
                Field fld = getField(o.getClass(), f);
                //do not store static fields!
                if (Modifier.isStatic(fld.getModifiers())) {
                    continue;
                }
                if (fld.isAnnotationPresent(Id.class)) {
                    fName = "_id";
                }
                if (fld == null) {
                    log.error("Field not found " + f);
                } else {
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
                            v = getId(value);
                            if (v == null) {
                                //not stored yet
                                if (r.automaticStore()) {
                                    //TODO: this could cause an endless loop!
                                    if (morphium == null) {
                                        log.fatal("Could not store - no Morphium set!");
                                    } else {
                                        morphium.store(value);
                                    }
                                } else {
                                    throw new IllegalArgumentException("Reference to be stored, that is null!");
                                }
                                v = getId(value);

                            }
                        }
                    } else {

                        //check, what type field has

                        //Store Entities recursively
                        //TODO: Fix recursion - this could cause a loop!
                        if (fld.getType().isAnnotationPresent(Entity.class)) {
                            if (value != null) {
                                DBObject obj = marshall(value);
                                obj.removeField("_id");  //Do not store ID embedded!
                                v = obj;
                            }
                        } else {
                            v = value;
                            if (v != null) {
                                if (v instanceof Map) {
                                    //create MongoDBObject-Map
                                    BasicDBObject dbMap = createDBMap((Map) v);
                                    v = dbMap;
                                } else if (v instanceof List) {
                                    BasicDBList lst = createDBList((List) v);
                                    v = lst;
                                } else if (v instanceof Iterable) {
                                    BasicDBList lst = new BasicDBList();
                                    for (Object i : (Iterable) v) {
                                        lst.add(i);
                                    }
                                    v = lst;
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
                }

            } catch (IllegalAccessException exc) {
                log.fatal("Illegal Access to field " + f);
            }

        }
        return dbo;
    }

    private BasicDBList createDBList(List v) {
        BasicDBList lst = new BasicDBList();
        for (Object lo : ((List) v)) {
            if (lo.getClass().isAnnotationPresent(Entity.class)) {
                DBObject marshall = marshall(lo);
                marshall.put("class_name", lo.getClass().getName());
                lst.add(marshall);
            } else if (lo instanceof List) {
                lst.add(createDBList((List) lo));
            } else if (lo instanceof Map) {
                lst.add(createDBMap(((Map) lo)));
            } else {
                lst.add(lo);
            }
        }
        return lst;
    }

    private BasicDBObject createDBMap(Map v) {
        BasicDBObject dbMap = new BasicDBObject();
        for (Object k : ((Map) v).keySet()) {
            if (!(k instanceof String)) {
                log.warn("Map in Mongodb needs to have String as keys - using toString");
                k = k.toString();
                if (((String) k).contains(".")) {
                    log.warn(". not allowed as Key in Maps - converting to _");
                    k = ((String) k).replaceAll("\\.", "_");
                }
            }
            Object mval = ((Map) v).get(k);
            if (mval != null) {
                if (mval.getClass().isAnnotationPresent(Entity.class)) {
                    DBObject obj = marshall(mval);
                    obj.put("class_name", mval.getClass().getName());
                    mval = obj;
                } else if (mval instanceof Map) {
                    mval = createDBMap((Map) mval);
                } else if (mval instanceof List) {
                    mval = createDBList((List) mval);
                }
            }
            dbMap.put((String) k, mval);
        }
        return dbMap;
    }

    @Override
    public <T> T unmarshall(Class<T> cls, DBObject o) {
        try {
            if (o.get("class_name") != null || o.get("className")!=null) {
                log.info("overriding cls - it's defined in dbObject");
                try {
                    String cN=(String)o.get("class_name");
                    if (cN==null) {
                        cN = (String)o.get("className");
                    }
                    cls = (Class<T>) Class.forName(cN);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
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
                    ObjectId id = (ObjectId) o.get(f);
                    if (morphium == null) {
                        log.fatal("Morphium not set - could not de-reference!");
                    } else {
                        Query q = morphium.createQueryFor(fld.getType());
                        q.f("_id").eq(id);
                        value = q.get();
                    }
                } else if (fld.isAnnotationPresent(Id.class)) {
                    ObjectId id = (ObjectId) o.get("_id");

                    value = id;
                } else if (fld.getType().isAnnotationPresent(Entity.class)) {
                    //entity! embedded
                    if (o.get(f) != null) {
                        value = unmarshall(fld.getType(), (DBObject) o.get(f));
                    } else {
                        value = null;
                    }
                } else if (fld.getType().isAssignableFrom(Map.class)) {
                    BasicDBObject map = (BasicDBObject) o.get(f);
                    if (map != null) {
                        for (String n : map.keySet()) {
                            //TODO: recurse?
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
                                }
                            }
                        }
                        value = map;
                    } else {
                        value = null;
                    }
                } else if (fld.getType().isAssignableFrom(List.class)) {
                    //TODO: recurse??
                    BasicDBList l = (BasicDBList) o.get(f);
                    List lst = new ArrayList();
                    if (l != null) {
                        for (Object val : l) {
                            if (val instanceof BasicDBObject) {
                                if (((BasicDBObject) val).containsField("class_name") || ((BasicDBObject) val).containsField("className")) {
                                    //Entity to map!
                                    String cn = (String) ((BasicDBObject) val).get("class_name");
                                    if (cn == null) {
                                        cn = (String) ((BasicDBObject) val).get("className");
                                    }
                                    try {
                                        Class ecls = Class.forName(cn);
                                        lst.add(unmarshall(ecls, (DBObject) val));
                                    } catch (ClassNotFoundException e) {
                                        throw new RuntimeException(e);
                                    }
                                } else {
                                    //Probably an "normal" map
                                    lst.add(val);
                                }
                            } else if (val instanceof DBRef) {
                                //todo: implement something
                                lst.add(((DBRef) val).getId());
                            } else {
                                lst.add(val);
                            }
                        }
                        value = lst;
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
            flds = getFields(cls, Id.class);
            if (flds.isEmpty()) {
                throw new RuntimeException("Error - class does not have an ID field!");
            }
            getField(cls, flds.get(0)).set(ret, o.get("_id"));
            return ret;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        //recursively fill class

    }

    @Override
    public ObjectId getId(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("Object cannot be null");
        }
        List<String> flds = getFields(o.getClass(), Id.class);
        if (flds == null || flds.isEmpty()) {
            throw new IllegalArgumentException("Object has no id defined: " + o.getClass().getSimpleName());
        }
        Field f = getField(o.getClass(), flds.get(0)); //first Id
        if (f == null) {
            throw new IllegalArgumentException("Object ID field not found " + o.getClass().getSimpleName());
        }
        try {
            if (!(f.getType().equals(ObjectId.class))) {
                throw new IllegalArgumentException("ID sould be of type ObjectId");
            }
            return (ObjectId) f.get(o);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Field> getAllFields(Class cls) {
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
        Entity entity = (Entity) sc.getAnnotation(Entity.class);
        boolean tcc = entity.translateCamelCase();
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
    public String getFieldName(Class cls, String field) {
        Field f = getField(cls, field);

        if (f.isAnnotationPresent(Property.class)) {
            Property p = f.getAnnotation(Property.class);
            if (p.fieldName() != null && !p.fieldName().equals(".")) {
                return p.fieldName();
            }
        }


        String fieldName = f.getName();
        Entity ent = (Entity) cls.getAnnotation(Entity.class);
        if (ent.translateCamelCase()) {
            fieldName = convertCamelCase(fieldName);
        }
        return fieldName;

    }

    @Override
    /**
     * extended logic: Fld may be, the java field name, the name of the specified value in Property-Annotation or
     * the translated underscored lowercase name (mongoId => mongo_id) or a name specified in the Aliases-Annotation of this field
     *
     * @param cls - class to search
     * @param fld - field name
     * @return field, if found, null else
     */
    public Field getField(Class cls, String fld) {
        Entity entity = (Entity) cls.getAnnotation(Entity.class);
        boolean tcc = entity.translateCamelCase();
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
                if (tcc && !convertCamelCase(f.getName()).equals(fld)) {
                    throw new IllegalArgumentException("Error camel casing! " + fld + " != " + convertCamelCase(f.getName()));
                }
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
        if (o instanceof Class) {
            return ((Class) o).isAnnotationPresent(Entity.class);
        }
        return o.getClass().isAnnotationPresent(Entity.class);
    }

    @Override
    public Object getValue(Object o, String fld) {
        if (o == null) {
            return null;
        }
        try {
            Field f = getField(o.getClass(), fld);
            if (!Modifier.isStatic(f.getModifiers())) {
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
            Field f = getField(o.getClass(), fld);
            if (!Modifier.isStatic(f.getModifiers())) {
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
                    }

                }
            }
        } catch (Exception e) {
            log.fatal("Illegal access to field " + fld + " of toype " + o.getClass().getSimpleName());
            return;
        }
    }
}
