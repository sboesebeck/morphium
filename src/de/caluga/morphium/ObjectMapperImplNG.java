package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.mapping.BigIntegerTypeMapper;
import de.caluga.morphium.mapping.MorphiumIdMapper;
import org.bson.types.ObjectId;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;
import sun.reflect.ReflectionFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 17.07.18
 * Time: 20:45
 * <p>
 * TODO: Add documentation here
 */
public class ObjectMapperImplNG implements ObjectMapper {
    private final List<Object> mongoTypes;
    private Morphium morphium;
    private AnnotationAndReflectionHelper anhelper;
    private Map<Class<?>, NameProvider> nameProviderByClass;
    private Map<Class, TypeMapper> customTypeMapper;
    private final ContainerFactory containerFactory;
    private final JSONParser jsonParser = new JSONParser();
    private final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();

    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;

    private Logger log = LoggerFactory.getLogger(ObjectMapperImplNG.class);

    public ObjectMapperImplNG() {
        nameProviderByClass = new ConcurrentHashMap<>();
        customTypeMapper = new ConcurrentHashMap<>();
        containerFactory = new ContainerFactory() {
            @Override
            public Map createObjectContainer() {
                return new HashMap<>();
            }

            @Override
            public List creatArrayContainer() {
                return new ArrayList();
            }
        };
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
        customTypeMapper.put(BigInteger.class, new BigIntegerTypeMapper());
        customTypeMapper.put(MorphiumId.class, new MorphiumIdMapper());
    }

    @Override
    public String getCollectionName(Class cls) {
        Entity p = anhelper.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) cls.getAnnotation(Entity.class);
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }
        try {
            cls = anhelper.getRealClass(cls);
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


    private NameProvider getNameProviderForClass(Class<?> cls, Entity p) throws IllegalAccessException, InstantiationException {
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }

        if (nameProviderByClass.get(cls) == null) {
            NameProvider np = p.nameProvider().newInstance();
            setNameProviderForClass(cls, np);
        }
        return nameProviderByClass.get(cls);
    }


    @Override
    public void registerCustomTypeMapper(Class c, TypeMapper m) {
        customTypeMapper.put(c, m);
    }

    @Override
    public Map<String, Object> marshall(Object o) {
        return marshall(o, ignoreEntity, ignoreReadOnly);
    }

    @Override
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
            if (!o.getClass().getComponentType().equals(byte.class)) {
                List lst = new ArrayList<>();
                for (int i = 0; i < Array.getLength(o); i++) {
                    Object v = Array.get(o, i);
                    lst.add(handleListElementMarshalling(v));
                }
                return lst;
            } else {
                return o;
            }
        } else if (o instanceof Map) {
            Map m = (Map) o;
            Map target = new HashMap();
            for (Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
                Object v = entry.getValue();
                if (v == null) {
                    target.put(marshallIfNecessary(entry.getKey()), null);
                    continue;
                }
                if (v.getClass().isEnum()) {
                    Map map = createEnumMapMarshalling((Enum) v, v.getClass());
                    target.put(marshallIfNecessary(entry.getKey()), map);
                } else {
                    Object value = marshallIfNecessary(v);
                    if (anhelper.isEntity(v)) {
                        ((Map) value).put("class_name", v.getClass().getName());
                    }
                    target.put(marshallIfNecessary(entry.getKey()), value);
                }
            }
            return target;
        }


        return marshall(o, true, ignoreReadOnly);

    }

    private Map createEnumMapMarshalling(Enum v, Class<?> aClass) {
        Map map = new HashMap();
        map.put("class_name", aClass.getName());
        map.put("name", v.name());
        return map;
    }

    private Object handleListElementMarshalling(Object v) {
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

    private Map<String, Object> marshall(Object o, boolean ignoreEntity, boolean ignoreReadOnly) {
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
                String fName = f;
                String key = anhelper.getFieldName(cls, fName);
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
            }
        } catch (IllegalAccessException e1) {
            //TODO: Implement Handling
            throw new RuntimeException(e1);
        }

        return dbo;
    }

    @Override
    public <T> T unmarshall(Class<? extends T> theClass, Map<String, Object> o) {
        if (o == null) return null;

        /////////////////////////////////////////
        ///getting type
        //

        Class cls = theClass;


        try {
            String cN = (String) o.get("class_name");
            if (cN == null) {
                cN = (String) o.get("className");
            }
            if (cN != null) {
                cls = Class.forName(cN);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        if (cls == null) {
            log.warn("Could not find type for map, assuming it is just a map");
            return (T) o;
        }
        if (customTypeMapper.get(cls) != null) {
            return (T) customTypeMapper.get(cls).unmarshall(o);
        }
        if (cls.isEnum()) {
            T[] en = (T[]) cls.getEnumConstants();
            for (Enum e : ((Enum[]) en)) {
                if (o instanceof Map) {
                    if (e.name().equals(o.get("name"))) {
                        return (T) e;
                    }
                } else {
                    if (e.name().equals(o)) {
                        return (T) e;
                    }
                }
            }
        }
        if (Map.class.isAssignableFrom(cls)) {
            return (T) o;
        }

        T result = null;
        try {
            result = (T) cls.newInstance();
        } catch (Exception ignored) {
        }
        if (result == null) {
            final Constructor<Object> constructor;
            try {
                constructor = (Constructor<Object>) reflection.newConstructorForSerialization(
                        cls, Object.class.getDeclaredConstructor());
                result = (T) constructor.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("could not instanciate class " + cls.getSimpleName(), e);
            }
        }

        ///////////////////////////////////////
        ///recursing through fields
        //
        //
        List<Field> fields = anhelper.getAllFields(cls);
        for (Field fld : fields) {
            try {

                fld.setAccessible(true);
                Class fieldType = fld.getType();
                String fName = anhelper.getFieldName(cls, fld.getName());
                if (customTypeMapper.get(fieldType) != null) {
                    try {
                        fld.set(result, customTypeMapper.get(fieldType).unmarshall(o.get(fName)));
                    } catch (IllegalAccessException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                    continue;
                }
                if (o.get(fName) == null) {
                    if (!fieldType.isPrimitive()) {
                        fld.set(result, null);
                    }
                    continue;
                }
                Reference r = fld.getAnnotation(Reference.class);
                boolean isentity = anhelper.isAnnotationPresentInHierarchy(fieldType, Embedded.class) || anhelper.isAnnotationPresentInHierarchy(fieldType, Entity.class);
                if (fieldType.isArray() && fieldType.getComponentType().isPrimitive()) {
                    //should get Primitives from DB as well
                    Object valueFromDb = o.get(fName);
                    if (valueFromDb instanceof Map && ((Map) valueFromDb).isEmpty()) {
                        fld.set(result, null);
                    } else {
                        if (valueFromDb.getClass().isArray() && valueFromDb.getClass().getComponentType().isPrimitive()) {
                            fld.set(result, valueFromDb);

                        } else {
                            Object arr = Array.newInstance(fieldType.getComponentType(), ((List) valueFromDb).size());

                            int count = 0;
                            if (fieldType.getComponentType().equals(int.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.intValue());
                                }
                            } else if (fieldType.getComponentType().equals(double.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.doubleValue());
                                }
                            } else if (fieldType.getComponentType().equals(float.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.floatValue());
                                }
                            } else if (fieldType.getComponentType().equals(boolean.class)) {
                                for (Boolean i : (List<Boolean>) valueFromDb) {
                                    Array.set(arr, count++, i.booleanValue());
                                }
                            } else if (fieldType.getComponentType().equals(byte.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.byteValue());
                                }
                            } else if (fieldType.getComponentType().equals(char.class)) {
                                for (Character i : (List<Character>) valueFromDb) {
                                    Array.set(arr, count++, i.charValue());
                                }
                            } else if (fieldType.getComponentType().equals(short.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.shortValue());
                                }
                            } else if (fieldType.getComponentType().equals(long.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.longValue());
                                }

                            }
                            fld.set(result, arr);
                        }
                    }
                    ///////////////////
                    ///////
                    //// List handling
                    //
                } else if (List.class.isAssignableFrom(fieldType) || fieldType.isArray()) {
                    List lst = (List) o.get(fName);
                    List resList = new ArrayList();

                    ParameterizedType type;
                    if (fld.getGenericType() instanceof ParameterizedType) {
                        type = (ParameterizedType) fld.getGenericType();
                    } else
                    // a real array! time to create a custom parameterized type!
                    {
                        type = new ParameterizedType() {

                            @Override
                            public Type getRawType() {
                                return Array.class;
                            }

                            @Override
                            public Type getOwnerType() {
                                return null;
                            }

                            @Override
                            public Type[] getActualTypeArguments() {
                                return new Type[]{fld.getType().getComponentType()};
                            }
                        };
                    }
                    for (Object listElement : lst) {
                        if (listElement == null) {
                            resList.add(null);
                            continue;
                        }
                        Class elementType = null;
                        if (listElement instanceof List) {
                            //sub-list
                            if (type.getActualTypeArguments()[0] instanceof ParameterizedType) {
                                ParameterizedType p = (ParameterizedType) type.getActualTypeArguments()[0];
                                elementType = (Class) p.getActualTypeArguments()[0];
                            }
                            List inner = new ArrayList();
                            for (Object innerListElem : (List) listElement) {
                                inner.add(unmarshallIfPossible(elementType, innerListElem));
                            }
                            resList.add(inner);
                            continue;
                        }

                        if (type.getActualTypeArguments()[0] instanceof WildcardType) {
                            WildcardType wt = (WildcardType) type.getActualTypeArguments()[0];
                            elementType = (Class) wt.getUpperBounds()[0];
//                        } else if (type.getActualTypeArguments()[0] instanceof ParameterizedType){
//                            ParameterizedType p=(ParameterizedType)type.getActualTypeArguments()[0];
//                                elementType=(Class)p.getActualTypeArguments()[0];

                        } else {
                            elementType = (Class) type.getActualTypeArguments()[0];
                        }
                        if (anhelper.isEntity(elementType)) {
                            if (r != null) {
                                //MorphiumReference ref = unmarshall(MorphiumReference.class, (Map<String, Object>) o.get(fName));
                                Map<String, Object> map = (Map<String, Object>) listElement;
                                if (r.lazyLoading()) {
                                    resList.add(morphium.createLazyLoadedEntity(elementType, map.get("id"), result, fName, (String) map.get("collection_name")));
                                } else {
                                    morphium.fireWouldDereference(result, fName, map.get("id"), fieldType, false);
                                    Object deref = morphium.findById(elementType, map.get("id"), (String) map.get("collection_name"));
                                    resList.add(deref);
                                    morphium.fireDidDereference(result, fName, deref, false);
                                }
                            } else {
                                resList.add(unmarshallIfPossible(elementType, listElement));
                            }
                        } else {
                            resList.add(unmarshallIfPossible(elementType, listElement));
                        }


                    }
                    if (fieldType.isArray()) {
                        fld.set(result, resList.toArray());
                    } else {
                        fld.set(result, resList);
                    }

                    ////////////////////////////////////////
                    ///////
                    ///   Map Value
                    //
                } else if (Map.class.isAssignableFrom(fieldType)) {
                    Map map = (Map) o.get(fName);
                    Map resMap = new HashMap();
                    for (Map.Entry en : (Set<Map.Entry>) map.entrySet()) {
                        if (en.getValue() instanceof List) {
                            List sublist = new ArrayList();
                            for (Object el : (List) en.getValue()) {
                                sublist.add(unmarshallIfPossible(null, el));
                            }
                            resMap.put(unmarshallIfPossible(null, en.getKey()), sublist);
                        } else {
                            resMap.put(unmarshallIfPossible(null, en.getKey()), unmarshallIfPossible(null, en.getValue()));
                        }

                    }
                    fld.set(result, resMap);
                } else {

                    //////////////////////////////
                    ////////
                    ///// just a field
                    ///
                    Object value = unmarshallIfPossible(fieldType, o.get(fName));
                    if (isentity) {
                        if (fld.isAnnotationPresent(Reference.class)) {
                            Map ref = (Map) o.get(fName);
                            //MorphiumReference mr=unmarshall(MorphiumReference.class,ref);
                            if (r.lazyLoading()) {
                                value = morphium.createLazyLoadedEntity(fld.getType(), ref.get("id"), result, fName, ref.get("collection_name").toString());
                            } else {
                                value = morphium.findById(fld.getType(), ref.get("id"), ref.get("collection_name").toString());
                            }

                        } else {
                            try {
                                Field idf = anhelper.getIdField(fieldType);
                                idf.setAccessible(true);
                                idf.set(value, null);
                            } catch (Exception e) {
                                //swallow
                            }
                        }
                    } else if (value != null && !fieldType.isAssignableFrom(value.getClass())) {
                        if (value instanceof String) {
                            log.info("Got String but have somethign different");
                            if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = Integer.valueOf((String) value);
                            } else if (fieldType.equals(Double.class) || fieldType.equals(Double.class)) {
                                value = Double.valueOf((String) value);
                            } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = Long.valueOf((String) value);

                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = Float.valueOf((String) value);
                            } else if (fieldType.equals(ObjectId.class)) {
                                value = new ObjectId((String) value);
                            } else if (fieldType.equals(MorphiumId.class)) {
                                value = new MorphiumId((String) value);
                            }
                        } else if (value instanceof Long) {
                            if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = ((Long) value).intValue();
                            } else if (fieldType.equals(Double.class) || fieldType.equals(Double.class)) {
                                value = ((Long) value).doubleValue();
                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = ((Long) value).floatValue();
                            } else {
                                //log.warn("Try string conversion to Long");
                                value = Long.valueOf(value.toString());
                            }
                        } else if (value instanceof Integer) {
                            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = ((Integer) value).longValue();
                            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                                value = ((Integer) value).doubleValue();
                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = ((Integer) value).floatValue();
                            } else {
                                //log.warn("Try string conversion to Integer");
                                value = Integer.valueOf(value.toString());
                            }
                        } else if (value instanceof Float) {
                            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = ((Float) value).longValue();
                            } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = ((Float) value).intValue();
                            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                                value = ((Float) value).doubleValue();
                            } else {
                                //log.warn("Try string conversion to Float");
                                value = Float.valueOf(value.toString());
                            }
                        } else if (value instanceof Double) {
                            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = ((Double) value).longValue();
                            } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = ((Double) value).intValue();
                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = ((Double) value).floatValue();
                            } else {
                                //log.warn("Try string conversion to Double");
                                value = Double.valueOf(value.toString());
                            }
                        }
                    }
                    fld.set(result, value);
                }
            } catch (IllegalAccessException e) {
                //TODO: Implement Handling
                throw new RuntimeException(e);

            }
        }
        if (anhelper.isAnnotationPresentInHierarchy(cls, Embedded.class)) {
            try {
                Field f = anhelper.getIdField(result);
                if (f != null) {
                    f.setAccessible(true);
                    try {
                        f.set(result, null);
                    } catch (IllegalAccessException e) {
                        log.error("Could not erase id from embedded entity", e);
                    }
                }
            } catch (Exception e) {
                //e.printStackTrace();
                //swallow
            }
        }
        return result;
    }

    private Object unmarshallIfPossible(Class fieldType, Object o) {
        if (o == null) return null;
        for (TypeMapper m : customTypeMapper.values()) {
            if (m.matches(o)) {
                return m.unmarshall(o);
            }
        }
        if (fieldType == null && o instanceof Map) {
            String cname = (String) ((Map) o).get("class_name");
            if (cname != null) {
                try {
                    fieldType = Class.forName(cname);
                } catch (ClassNotFoundException e) {
                    //TODO: Implement Handling
                    throw new RuntimeException(e);
                }
            }
        }
        if (fieldType != null && fieldType.isEnum()) {
            String n = o.toString();
            if (o instanceof Map) {
                //in list or map
                n = (String) ((Map) o).get("name");
            }
            Enum en[] = (Enum[]) fieldType.getEnumConstants();
            for (Enum e : en) {
                if (e.name().equals(n)) {
                    return e;
                }
            }
            throw new IllegalArgumentException("Enum constant not found: " + n);
        }
        if (o instanceof Map) {
            //only this could be a more complex object
            return unmarshall(fieldType, (Map) o);
        }
        if (fieldType != null && fieldType.isPrimitive() && o == null) {
            return 0;
        }
        //conversion
        if (fieldType != null && o != null) {
            if (fieldType.equals(String.class) && o instanceof Integer) {
                return o.toString();
            }
        }
        return o;
    }

    @Override
    public <T> T unmarshall(Class<? extends T> cls, String json) throws ParseException {

        HashMap<String, Object> obj = (HashMap<String, Object>) jsonParser.parse(json, containerFactory);
        return unmarshall(cls, obj);

    }

    @Override
    public NameProvider getNameProviderForClass(Class<?> cls) {
        return nameProviderByClass.get(cls);
    }

    @Override
    public void setNameProviderForClass(Class<?> cls, NameProvider np) {
        nameProviderByClass.put(cls, np);
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        if (m != null) {
            anhelper = m.getARHelper();
        } else {
            anhelper = new AnnotationAndReflectionHelper(true);
        }
    }

    @Override
    public void setAnnotationHelper(AnnotationAndReflectionHelper an) {
        anhelper = an;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    @Override
    public void deregisterTypeMapper(Class c) {
        customTypeMapper.remove(c);
    }
}
