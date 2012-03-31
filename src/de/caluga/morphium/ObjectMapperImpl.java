package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.annotations.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:26
 * <p/>
 */
public class ObjectMapperImpl implements ObjectMapper {
    private static Logger log = Logger.getLogger(ObjectMapperImpl.class);

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

        for (String f : flds) {
            String fName = f;
            try {
                Field fld = getField(o.getClass(), f);
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
                                    Morphium.get().store(value);
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
                                    v = new BasicDBObject((Map) v);
                                } else if (v instanceof List) {
                                    BasicDBList lst = new BasicDBList();
                                    lst.addAll((List) v);
                                    v = lst;
                                } else if (v instanceof Iterable) {
                                    BasicDBList lst = new BasicDBList();
                                    for (Object i : (Iterable) v) {
                                        lst.add(i);
                                    }
                                    v = lst;
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

            } catch (IllegalAccessException e) {
                log.fatal("Illegal Access to field " + f);
            }

        }
        return dbo;
    }

    @Override
    public <T> T unmarshall(Class<T> cls, DBObject o) {
        try {
            T ret = cls.newInstance();
            List<String> flds = getFields(cls);

            for (String f : flds) {
                Field fld = getField(cls, f);
                Object value = null;
                if (fld.isAnnotationPresent(Reference.class)) {
                    //A reference - only id stored
                    ObjectId id = (ObjectId) o.get(f);
                    Query q = Morphium.get().createQueryFor(fld.getType());
                    q.f("_id").eq(id);
                    value = q.get();
                } else if (fld.isAnnotationPresent(Id.class)) {
                    ObjectId id = (ObjectId) o.get("_id");

                    value = id;
                } else if (fld.getType().isAnnotationPresent(Entity.class)) {
                    //entity! embedded
                    value = unmarshall(fld.getType(), (DBObject) o.get(f));
                } else if (fld.getType().isInstance(Map.class)) {
                    BasicDBObject map = (BasicDBObject) o.get(f);
                    value = map;
                } else if (fld.getType().isInstance(List.class)) {
                    BasicDBList l = (BasicDBList) o.get(f);
                    value = l;
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

    @Override
    /**
     * get a list of valid fields of a given record as they are in the MongoDB
     * so, if you have a field Mapping, the mapped Property-name will be used
     * returns all fields, which have at least one of the given annotations
     * if no annotation is given, all fields are returned
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
                if (f.isAnnotationPresent(Id.class)) {
                    ret.add(f.getName());
                    continue;
                }
                if (f.isAnnotationPresent(Transient.class)) {
                    continue;
                }

                if (tcc) {
                    ret.add(convertCamelCase(f.getName()));
                } else {
                    ret.add(f.getName());
                }
            }
        }
        return ret;
    }

    @Override
    /**
     * extended logic: Fld may be, the java field name, the name of the specified value in Property-Annotation or
     * the translated underscored lowercase name (mongoId => mongo_id)
     *
     * @param cls - class to search
     * @param fld - field name
     * @return field, if found, null else
     */
    public Field getField(Class cls, String fld) {
        List<String> ret = new Vector<String>();
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
            return getField(o.getClass(), fld).get(o);
        } catch (IllegalAccessException e) {
            log.fatal("Illegal access to field " + fld + " of toype " + o.getClass().getSimpleName());
            return null;
        }
    }

    @Override
    public void setValue(Object o, Object value, String fld) {
        if (o == null) {
            return;
        }
        try {
            getField(o.getClass(), fld).set(o, value);
        } catch (IllegalAccessException e) {
            log.fatal("Illegal access to field " + fld + " of toype " + o.getClass().getSimpleName());
            return;
        }
    }
}
