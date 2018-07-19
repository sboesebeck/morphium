package de.caluga.morphium.objectmapper;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public class MarshallHelper {

    private final List<Object> mongoTypes;
    private final AnnotationAndReflectionHelper anhelper;
    private final Map<Class<?>, NameProvider> nameProviderByClass;
    private final Map<Class, TypeMapper> customTypeMapper;
    private final Morphium morphium;
    private final ObjectMapper objectMapper;
    private final Logger log = LoggerFactory.getLogger(MarshallHelper.class);
    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;

    public MarshallHelper(AnnotationAndReflectionHelper ar, Map<Class, TypeMapper> customMapper, Map<Class<?>, NameProvider> np, Morphium m, ObjectMapper om) {
        mongoTypes = Collections.synchronizedList(new ArrayList<>());

        mongoTypes.add(String.class);
        mongoTypes.add(Character.class);
        mongoTypes.add(Integer.class);
        mongoTypes.add(Long.class);
        mongoTypes.add(Float.class);
        mongoTypes.add(Double.class);
        mongoTypes.add(Date.class);
        mongoTypes.add(Boolean.class);
        mongoTypes.add(Byte.class);
        anhelper = ar;
        customTypeMapper = customMapper;
        nameProviderByClass = np;
        morphium = m;

        objectMapper = om;
    }


    public Object marshallIfNecessary(Object o) {
        if (o == null) return null;
        Class valueType = anhelper.getRealClass(o.getClass());
        //primitives
        if (valueType.isPrimitive()) {
            return o;
        }

        //"primitive" values
        if (mongoTypes.contains(valueType)) {
            return o;
        }

        //custom Mapper
        if (customTypeMapper.get(valueType) != null) {
            return customTypeMapper.get(valueType).marshall(o);
        }

        //Enums
        if (valueType.isEnum()) {
            return ((Enum) o).name();
        }

        if (o instanceof Collection) {
            Collection c = (Collection) o;
            List lst = new ArrayList();
            for (Object elem : c) {
                lst.add(handleListElementMarshalling(elem));
            }
            return lst;
        } else if (o instanceof ObjectId) {
            if (valueType.equals(MorphiumId.class)) {
                return new MorphiumId(((ObjectId) o).toByteArray());
            } else if (valueType.equals(String.class)) {
                return o.toString();
            } else {
                throw new IllegalArgumentException("cannot handle objectIds: type of field is " + valueType.getName());
            }
        } else if (valueType.isArray()) {
            if (o.getClass().getComponentType().equals(byte.class)) {
                return o;
            } else {
                List lst = new ArrayList<>();
                for (int i = 0; i < Array.getLength(o); i++) {
                    Object v = Array.get(o, i);
                    lst.add(handleListElementMarshalling(v));
                }
                return lst;
            }
        } else if (o instanceof Map) {
            Map m = (Map) o;
            Map target = new HashMap();
            for (Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
                Object v = entry.getValue();
                if (v == null) {
                    target.put(entry.getKey(), null);
                    continue;
                }
                if (v.getClass().isEnum()) {
                    Map map = createEnumMapMarshalling((Enum) v, v.getClass());
                    target.put(entry.getKey(), map);
                } else {
                    Object value = marshallIfNecessary(v);
                    if (anhelper.isEntity(v)) {
                        ((Map) value).put("class_name", v.getClass().getName());
                    }
                    target.put(entry.getKey(), value);
                }
            }
            return target;
        }


        return marshall(o, true, ignoreReadOnly);

    }


    public Object handleListElementMarshalling(Object v) {
        if (v == null) return null;
        if (v.getClass().isEnum()) {
            Map m = createEnumMapMarshalling((Enum) v, v.getClass());
            return m;
        } else {
            Object ret = marshallIfNecessary(v);
            if (anhelper.isEntity(v)) {
                ((Map) ret).put("class_name", v.getClass().getName());
            }
            return ret;
        }

    }


    public Map<String, Object> marshall(Object o, boolean ignoreEntity, boolean ignoreReadOnly) {
        if (o == null) return null;
        if (customTypeMapper.get(o.getClass()) != null) {
            return (Map<String, Object>) customTypeMapper.get(o.getClass()).marshall(o);
        }
        //recursively map object to mongo-Object...
        if (!anhelper.isEntity(o) && !ignoreEntity) {
            if (morphium == null || morphium.getConfig().isObjectSerializationEnabled()) {
                if (o instanceof Serializable) {
                    try {
                        BinarySerializedObject obj = new BinarySerializedObject();
                        obj.setOriginalClassName(o.getClass().getName());
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        ObjectOutputStream oout = new ObjectOutputStream(out);
                        oout.writeObject(o);
                        oout.flush();

                        BASE64Encoder enc = new BASE64Encoder();

                        String str = enc.encode(out.toByteArray());
                        obj.setB64Data(str);
                        return marshall(obj, false, false);

                    } catch (IOException e) {
                        throw new IllegalArgumentException("Binary serialization failed! " + o.getClass().getName(), e);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot write object to db that is neither entity, embedded nor serializable! ObjectType: " + o.getClass().getName());
                }
            }
            throw new IllegalArgumentException("Object is no entity: " + o.getClass().getSimpleName());
        }


        HashMap<String, Object> dbo = null;
        try {
            dbo = new HashMap<>();

            Class<?> cls = anhelper.getRealClass(o.getClass());
            if (cls == null) {
                throw new IllegalArgumentException("No real class?");
            }
            o = anhelper.getRealObject(o);
            List<String> flds = anhelper.getFields(cls, ignoreEntity);
            if (flds == null) {
                throw new IllegalArgumentException("Fields not found? " + cls.getName());
            }
            Entity e = anhelper.getAnnotationFromHierarchy(cls, Entity.class); //o.getClass().getAnnotation(Entity.class);
            Embedded emb = anhelper.getAnnotationFromHierarchy(cls, Embedded.class); //o.getClass().getAnnotation(Embedded.class);

            if (e != null && e.polymorph()) {
                dbo.put("class_name", cls.getName());
            }

            if (emb != null && emb.polymorph()) {
                dbo.put("class_name", cls.getName());
            }


            for (String f : flds) {

                Field fld = anhelper.getField(cls, f);
                if (fld == null) {
                    log.error("Field not found");
                    continue;
                }
                //do not store static fields!
                if (Modifier.isStatic(fld.getModifiers())) {
                    continue;
                }
                String key = anhelper.getFieldName(cls, fld.getName());
                if (fld.isAnnotationPresent(Id.class)) {
                    key = "_id";
                }
                //read only fields should probably not be mapped
                //main purpose of this mapper is to prepare a map to be stored to mongo
                //hence: readonly fields should not be mapped - unless you override that setting
                if (fld.isAnnotationPresent(ReadOnly.class) && !ignoreReadOnly) {
                    continue; //do not write value
                }
                Object value = fld.get(o);
                Class<?> type = fld.getType();
                UseIfnull useIfnull = anhelper.getAnnotationFromHierarchy(type, UseIfnull.class);

                if (value == null) {
                    if (useIfnull != null) {
                        dbo.put(key, null);
                    }
                    continue;
                }
                Reference referenceAnnotation = fld.getAnnotation(Reference.class);
                if (referenceAnnotation != null) {
                    if (List.class.isAssignableFrom(type)) {
                        //Reference list
                        List l = (List) value;
                        List resList = new ArrayList();
                        for (Object listEl : l) {
                            if (listEl == null) {
                                resList.add(null);
                                continue;
                            }
                            Map v = (Map) marshallIfNecessary(listEl);
                            String collection = referenceAnnotation.targetCollection();
                            if (collection.equals(".")) {
                                collection = getCollectionName(listEl.getClass());
                            }

                            Object id = v.get("_id");
                            if (id instanceof ObjectId) {
                                id = new MorphiumId(((ObjectId) id).toByteArray());
                            }
//                            MorphiumReference ref = new MorphiumReference(fld.getType().getName(), id);
//                            ref.setCollectionName(collection);
//                            value = marshall(ref);
                            if (id == null) {
                                id = automaticStore(referenceAnnotation, listEl);
                            }
                            Map ref = new HashMap();
                            ref.put("collection_name", collection);
                            ref.put("referenced_class_name", type.getName());
                            ref.put("id", id);
                            value = ref;
                            resList.add(value);
                        }
                        dbo.put(key, resList);
                        continue;
                    } else {
                        //reference field
                        Map v = marshall(value);
                        String collection = referenceAnnotation.targetCollection();
                        if (collection.equals(".")) {
                            collection = getCollectionName(type);
                        }
                        Object id = v.get("_id");
                        if (id instanceof ObjectId) {
                            id = new MorphiumId(((ObjectId) id).toByteArray());
                        }
                        if (id == null) {
                            automaticStore(referenceAnnotation, value);
                        }
//                        MorphiumReference ref = new MorphiumReference(fld.getType().getName(), id);
//                        ref.setCollectionName(collection);
                        Map ref = new HashMap();
                        ref.put("collection_name", collection);
                        ref.put("referenced_class_name", type.getName());
                        ref.put("id", id);
                        value = ref;
                        dbo.put(key, value);
                        continue;
                    }
                }
                dbo.put(key, marshallIfNecessary(value));
                if (dbo.get(key) instanceof Map) {
                    //remove ID if embedded
                    ((Map) dbo.get(key)).remove("_id");
                }
            }
        } catch (IllegalAccessException e1) {
            //TODO: Implement Handling
            throw new RuntimeException(e1);
        }

        return dbo;
    }

    public Map<String, Object> marshall(Object o) {
        return marshall(o, ignoreEntity, ignoreReadOnly);
    }


    private NameProvider getNameProviderForClass(Class<?> cls, Entity p) throws IllegalAccessException, InstantiationException {
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }

        if (nameProviderByClass.get(cls) == null) {
            NameProvider np = p.nameProvider().newInstance();
            objectMapper.setNameProviderForClass(cls, np);
        }
        return nameProviderByClass.get(cls);
    }


    public String getCollectionName(Class cls) {
        Entity p = anhelper.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) cls.getAnnotation(Entity.class);
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }
        try {
            cls = anhelper.getRealClass(cls);
            NameProvider np = getNameProviderForClass(cls, p);
            return np.getCollectionName(cls, objectMapper, p.translateCamelCase(), p.useFQN(), p.collectionName().equals(".") ? null : p.collectionName(), morphium);
        } catch (InstantiationException e) {
            log.error("Could not instanciate NameProvider: " + p.nameProvider().getName(), e);
            throw new RuntimeException("Could not Instaciate NameProvider", e);
        } catch (IllegalAccessException e) {
            log.error("Illegal Access during instanciation of NameProvider: " + p.nameProvider().getName(), e);
            throw new RuntimeException("Illegal Access during instanciation", e);
        }
    }


    private Map createEnumMapMarshalling(Enum v, Class<?> aClass) {
        Map map = new HashMap();
        map.put("class_name", aClass.getName());
        map.put("name", v.name());
        return map;
    }


    private Object automaticStore(Reference r, Object rec) {
        Object id;
        if (r.automaticStore()) {
            if (morphium == null) {
                throw new RuntimeException("Could not automagically store references as morphium is not set!");
            }
            String coll = r.targetCollection();
            if (coll.equals(".")) {
                coll = null;
            }
            morphium.storeNoCache(rec, coll);
            id = anhelper.getId(rec);
        } else {
            throw new IllegalArgumentException("Cannot store reference to unstored entity if automaticStore in @Reference is set to false!");
        }
        return id;
    }


}
