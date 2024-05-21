package de.caluga.morphium;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.encryption.ValueEncryptionProvider;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.objectmapping.*;
import de.caluga.morphium.query.geospatial.*;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassGraphException;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.ReflectionFactory;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 19:36
 * <p>
 */
@SuppressWarnings({"ConstantConditions", "MismatchedQueryAndUpdateOfCollection", "unchecked", "RedundantCast"})
public class ObjectMapperImpl implements MorphiumObjectMapper {
    private final Logger log = LoggerFactory.getLogger(ObjectMapperImpl.class);
    private final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();
    private final Map<Class<?>, NameProvider> nameProviders;
    //private final JSONParser jsonParser = new JSONParser();
    private final ObjectMapper jacksonOM = new ObjectMapper();

    private final ArrayList<Class<?>> mongoTypes;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper(true);
    private final Map<Class<?>, MorphiumTypeMapper> customMappers = new ConcurrentHashMap<>();
    private final Map<String, Class<?>> classByCollectionName = new ConcurrentHashMap<>();
    private Morphium morphium;

    public ObjectMapperImpl() {
        nameProviders = new ConcurrentHashMap<>();
        mongoTypes = new ArrayList<>();
        mongoTypes.add(String.class);
        mongoTypes.add(Integer.class);
        mongoTypes.add(Long.class);
        mongoTypes.add(Float.class);
        mongoTypes.add(Double.class);
        mongoTypes.add(Date.class);
        mongoTypes.add(Boolean.class);
        mongoTypes.add(Byte.class);
        mongoTypes.add(Pattern.class);
        mongoTypes.add(UUID.class);
        customMappers.put(Character.class, new CharacterMapper());
        customMappers.put(AtomicBoolean.class, new AtomicBooleanMapper());
        customMappers.put(AtomicInteger.class, new AtomicIntegerMapper());
        customMappers.put(AtomicLong.class, new AtomicLongMapper());
        customMappers.put(LocalDate.class, new LocalDateMapper());
        customMappers.put(LocalTime.class, new LocalTimeMapper());
        customMappers.put(LocalDateTime.class, new LocalDateTimeMapper());
        customMappers.put(Timestamp.class, new TimestampMapper());
        customMappers.put(Instant.class, new InstantMapper());
        customMappers.put(BigDecimal.class, new BigDecimalMapper());
        customMappers.put(BigInteger.class, new BigIntegerTypeMapper());
        customMappers.put(Geo.class, new BsonGeoMapper());
        customMappers.put(Point.class, new BsonGeoMapper());
        customMappers.put(MultiPoint.class, new BsonGeoMapper());
        customMappers.put(MultiLineString.class, new BsonGeoMapper());
        customMappers.put(MultiPolygon.class, new BsonGeoMapper());
        customMappers.put(Polygon.class, new BsonGeoMapper());
        customMappers.put(LineString.class, new BsonGeoMapper());

        //initializing type IDs
        try (ScanResult scanResult = new ClassGraph()
            //                     .verbose()             // Enable verbose logging
            .enableAnnotationInfo()
            //                             .enableAllInfo()       // Scan classes, methods, fields, annotations
            .scan()) {
            ClassInfoList entities = scanResult.getClassesWithAnnotation(Entity.class.getName());
            //entities.addAll(scanResult.getClassesWithAnnotation(Embedded.class.getName()));
            log.debug("Found " + entities.size() + " entities in classpath");

            for (String cn : entities.getNames()) {
                try {
                    Class c = Class.forName(cn);
                    classByCollectionName.put(getCollectionName(c), c);
                } catch (ClassNotFoundException e) {
                    log.error("Could not get class / collection " + cn);
                }
            }
        } catch (ClassGraphException e) {
            //swallow
        }
    }

    @Override
    public void setAnnotationHelper(AnnotationAndReflectionHelper an) {
        annotationHelper = an;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
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

    /**
     * override nameprovider in runtime!
     *
     * @param cls - class to use
     * @param np  - the NameProvider for that class
     */
    public void setNameProviderForClass(Class<?> cls, NameProvider np) {
        nameProviders.put(cls, np);
    }

    @Override
    public <T> void registerCustomMapperFor(Class<T> cls, MorphiumTypeMapper<T> map) {
        customMappers.put(cls, map);
    }

    @Override
    public void deregisterCustomMapperFor(Class cls) {
        customMappers.remove(cls);
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
            throw new IllegalArgumentException("could not get name provider", ex);
        }
    }

    private NameProvider getNameProviderForClass(Class<?> cls, Entity p) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if (p == null) {
            throw new IllegalArgumentException("No Entity " + cls.getSimpleName());
        }

        NameProvider np = nameProviders.get(cls);

        if (np == null) {
            np = p.nameProvider().getDeclaredConstructor().newInstance();
            nameProviders.put(cls, np);
        }

        return np;
    }

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
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            log.error("Illegal Access during instanciation of NameProvider: " + p.nameProvider().getName(), e);
            throw new RuntimeException("Illegal Access during instanciation", e);
        }
    }

    @SuppressWarnings("CommentedOutCode")
    public Object marshallIfNecessary(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof Expr) {
            return ((Expr) o).toQueryObject();
        }

        if (annotationHelper.isEntity(o) || customMappers.containsKey(o.getClass())) {
            return serialize(o);
        }

        if (o.getClass().isPrimitive()) {
            return o;
        }

        if (o.getClass().isArray()) {
            if (o.getClass().getComponentType().equals(byte.class)) {
                return o;
            }

            int arrayLength = Array.getLength(o);
            ArrayList lst = new ArrayList(arrayLength);

            for (int i = 0; i < arrayLength; i++) {
                lst.add(marshallIfNecessary(Array.get(o, i)));
            }

            return serializeIterable(lst, null, null);
        }

        if (o instanceof Iterable) {
            return serializeIterable((Iterable) o, null, null);
        }

        if (o instanceof Map) {
            return serializeMap((Map) o, null);
        }

        //        if (o instanceof MorphiumId) {
        //            o = new ObjectId(((MorphiumId) o).getBytes());
        //        }
        return o;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> serialize(Object o) {
        if (o == null) {
            return new HashMap<>();
        }

        Class<?> cls = annotationHelper.getRealClass(o.getClass());

        if (customMappers.containsKey(cls)) {
            Object ret = customMappers.get(cls).marshall(o);

            if (ret instanceof Map) {
                String typeIdForClass = null;

                if (getMorphium() != null) {
                    typeIdForClass = getMorphium().getARHelper().getTypeIdForClass(cls);
                } else {
                    typeIdForClass = cls.getName();
                }

                ((Map) ret).put("class_name", typeIdForClass);
                return (Map<String, Object>) ret;
            } else {
                return UtilsMap.of("value", ret);
            }
        }

        //recursively map object to mongo-Object...
        if (cls == null) {
            throw new IllegalArgumentException("No real class?");
        }

        o = annotationHelper.getRealObject(o);
        Entity e = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class);
        Embedded emb = annotationHelper.getAnnotationFromHierarchy(cls, Embedded.class);
        boolean objectIsEntity = e != null || emb != null;
        boolean warnOnNoEntitySerialization = morphium != null && morphium.getConfig() != null && morphium.getConfig().isWarnOnNoEntitySerialization();
        boolean objectSerializationEnabled = morphium == null || morphium.getConfig() == null || morphium.getConfig().isObjectSerializationEnabled();

        if (!objectIsEntity && !warnOnNoEntitySerialization) {
            if (objectSerializationEnabled) {
                if (o instanceof Serializable) {
                    try {
                        BinarySerializedObject obj = new BinarySerializedObject();
                        obj.setOriginalClassName(cls.getName());
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        ObjectOutputStream oout = new ObjectOutputStream(out);
                        oout.writeObject(o);
                        oout.flush();
                        Encoder enc = Base64.getMimeEncoder();
                        String str = new String(enc.encode(out.toByteArray()));
                        obj.setB64Data(str);
                        return serialize(obj);
                    } catch (IOException ex) {
                        throw new IllegalArgumentException("Binary serialization failed! " + cls.getName(), ex);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot write object to db that is neither entity, embedded nor serializable! ObjectType: " + cls.getName());
                }
            }

            throw new IllegalArgumentException("Object is no entity: " + cls.getSimpleName());
        }

        if (!objectIsEntity && warnOnNoEntitySerialization) {
            log.warn("Serializing non-entity of type " + cls.getName());
        }

        HashMap<String, Object> dbo = new HashMap<>();
        List<String> flds = annotationHelper.getFields(cls);

        if (flds == null) {
            throw new IllegalArgumentException("Fields not found? " + cls.getName());
        }

        String cn = cls.getName();

        if (e != null && !e.typeId().equals(".")) {
            cn = e.typeId();
        }

        if (emb != null && !emb.typeId().equals(".")) {
            cn = emb.typeId();
        }

        if (e != null && e.polymorph()) {
            dbo.put("class_name", cn);
        }

        if (emb != null && emb.polymorph()) {
            dbo.put("class_name", cn);
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

                Object value = fld.get(o);

                if (fld.isAnnotationPresent(Encrypted.class)) {
                    if (value == null) continue;

                    try {
                        Encrypted enc = fld.getAnnotation(Encrypted.class);
                        ValueEncryptionProvider encP = enc.provider().getDeclaredConstructor().newInstance();
                        byte[] encKey = morphium.getEncryptionKeyProvider().getEncryptionKey(enc.keyName());
                        encP.setEncryptionKey(encKey);

                        if (value instanceof String) {
                            byte[] encrypted = encP.encrypt(((String) value).getBytes());
                            dbo.put(fName, encrypted);
                        } else {
                            byte[] encrypted = encP.encrypt(Utils.toJsonString(marshallIfNecessary(value)).getBytes());
                            dbo.put(fName, encrypted);
                        }

                        continue;
                    } catch (Exception exc) {
                        throw new RuntimeException("Ecryption failed. Field: " + fName + " class: " + cls.getName(), exc);
                    }
                }

                AdditionalData ad = fld.getAnnotation(AdditionalData.class);

                if (ad != null) {
                    if (!ad.readOnly()) {
                        //storing additional data
                        if (value != null) {
                            dbo.putAll(serializeMap((Map) value, fld.getGenericType()));
                        }
                    }

                    //additional data is usually transient
                    continue;
                }

                if (dbo.containsKey(fName)) {
                    //already stored, skip it
                    log.warn("Field " + fName + " is shadowed - inherited values?");
                    continue;
                }

                Object v = null;

                if (fld.isAnnotationPresent(Id.class)) {
                    fName = "_id";
                }

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
                            List<Map<String, Object>> lst = new ArrayList<>();

                            for (Object rec : ((Collection) value)) {
                                if (rec != null) { //noinspection DuplicatedCode
                                    Object id = annotationHelper.getId(rec);

                                    if (id == null) {
                                        id = automaticStore(r, rec);
                                    }

                                    if (morphium == null) {
                                        throw new RuntimeException("cannot set dbRef - morphium is not set");
                                    }

                                    MorphiumReference ref = new MorphiumReference(annotationHelper.getTypeIdForClass(rec.getClass()), id);
                                    lst.add(serialize(ref));
                                } else {
                                    lst.add(null);
                                }
                            }

                            v = lst;
                        } else if (Map.class.isAssignableFrom(fld.getType())) {
                            //trying to store references
                            Map<Object, Map<String, Object>> map = new HashMap<>();
                            //noinspection DuplicatedCode
                            ((Map) value).forEach((key, rec) -> {
                                Object id = annotationHelper.getId(rec);

                                if (id == null) {
                                    id = automaticStore(r, rec);
                                }
                                if (morphium == null) {
                                    throw new RuntimeException("cannot set dbRef - morphium is not set");
                                }
                                MorphiumReference ref = new MorphiumReference(annotationHelper.getTypeIdForClass(rec.getClass()), id);
                                map.put(key, serialize(ref));
                            });
                            v = map;
                        } else {
                            if (annotationHelper.getId(value) == null) {
                                //not stored yet
                                if (r.automaticStore()) {
                                    //Attention: this could cause an endless loop!
                                    if (morphium == null) {
                                        log.error("Could not store - no Morphium set!");
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
                    Class<?> valueClass;

                    if (value == null) {
                        valueClass = fld.getType();
                    } else {
                        valueClass = value.getClass();
                    }

                    if (annotationHelper.isAnnotationPresentInHierarchy(valueClass, Entity.class)) {
                        if (value != null) {
                            Map<String, Object> obj = serialize(value);
                            obj.remove("_id");  //Do not store ID embedded!
                            v = obj;
                        }
                    } else if (annotationHelper.isAnnotationPresentInHierarchy(valueClass, Embedded.class)) {
                        if (value != null) {
                            v = serialize(value);
                        }
                    } else {
                        v = value;

                        if (v != null) {
                            if (v instanceof Map) {
                                //create MongoHashMap<String,Object>-Map
                                v = serializeMap((Map) v, fld.getGenericType());
                            } else if (v.getClass().isArray()) {
                                if (!v.getClass().getComponentType().equals(byte.class)) {
                                    int arrayLength = Array.getLength(v);
                                    ArrayList lst = new ArrayList(arrayLength);

                                    for (int i = 0; i < arrayLength; i++) {
                                        lst.add(Array.get(v, i));
                                    }

                                    v = serializeIterable(lst, fld.getType(), fld.getGenericType());
                                }
                            } else if (v instanceof Iterable) {
                                v = serializeIterable((Iterable) v, fld.getType(), fld.getGenericType());
                            } else if (v instanceof Calendar) {
                                v = ((Calendar) v).getTime();
                            } else if (v.getClass().equals(MorphiumId.class)) {
                                v = new ObjectId(((MorphiumId) v).getBytes());
                            } else if (customMappers.containsKey(v.getClass())) {
                                v = customMappers.get(v.getClass()).marshall(v);
                            } else if (v instanceof Enum) {
                                v = serializeEnum(fld.getType(), ((Enum) v));
                            } else if (!mongoTypes.contains(v.getClass()) && Serializable.class.isAssignableFrom(v.getClass())) {
                                //not a type the driver does understand, but is serializable
                                //try binary serialization
                                v = serialize(v);
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
                log.error("Illegal Access to field " + f);
            }
        }

        return dbo;
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
            id = annotationHelper.getId(rec);
        } else {
            throw new IllegalArgumentException("Cannot store reference to unstored entity if automaticStore in @Reference is set to false!");
        }

        return id;
    }

    public List<Object> serializeIterable(Iterable v, Class<?> collectionClass, Type collectionType) {
        Class elementClass = null;
        Type elementType = null;

        if (collectionType instanceof ParameterizedType) {
            elementClass = getElementClass((ParameterizedType) collectionType);
            elementType = getElementType((ParameterizedType) collectionType);
        } else if (collectionType instanceof GenericArrayType) {
            elementType = ((GenericArrayType) collectionType).getGenericComponentType();

            if (elementType instanceof Class) {
                elementClass = (Class) elementType;
            } else if (elementType instanceof ParameterizedType) {
                Type rawType = ((ParameterizedType) elementType).getRawType();

                if (rawType instanceof Class) {
                    elementClass = (Class) rawType;
                }
            }
        }

        if (collectionClass != null) {
            Class<?> componentType = collectionClass.getComponentType();

            if (componentType != null && elementClass == null) {
                elementClass = componentType;
            }
        }

        if (elementClass == null) {
            elementClass = Object.class;
        }

        List<Object> lst = new ArrayList<>();

        for (Object lo : v) {
            if (lo != null) {
                Class<?> loClass = lo.getClass();
                Entity loEntity = annotationHelper.getAnnotationFromHierarchy(loClass, Entity.class);
                Embedded loEmbedded = annotationHelper.getAnnotationFromHierarchy(loClass, Embedded.class);

                if (loEntity != null || loEmbedded != null) {
                    Map<String, Object> marshall = serialize(lo);
                    String cn = getTypeId(loClass, loEntity, loEmbedded);
                    marshall.put("class_name", cn);
                    lst.add(marshall);
                } else if (lo instanceof Iterable) {
                    lst.add(serializeIterable((Iterable) lo, elementClass, elementType));
                } else if (lo instanceof Map) {
                    lst.add(serializeMap(((Map) lo), elementType));
                } else if (lo instanceof MorphiumId) {
                    lst.add(new ObjectId(((MorphiumId) lo).getBytes()));
                } else if (lo instanceof Enum) {
                    lst.add(serializeEnum(elementClass, ((Enum) lo)));
                } else if (loClass.isPrimitive() || mongoTypes.contains(loClass)) {
                    lst.add(lo);
                } else if (loClass.isArray()) {
                    if (loClass.getComponentType().equals(byte.class)) {
                        lst.add(lo);
                    } else {
                        int arrayLength = Array.getLength(lo);
                        ArrayList loLst = new ArrayList(arrayLength);

                        for (int i = 0; i < arrayLength; i++) {
                            loLst.add(Array.get(lo, i));
                        }

                        lst.add(serializeIterable(loLst, elementClass, elementType));
                    }
                } else {
                    lst.add(serialize(lo));
                }
            } else {
                lst.add(null);
            }
        }

        return lst;
    }

    private String getTypeId(Enum enumVal) {
        Class<?> cls = enumVal.getClass();
        Class<?> superclass = cls.getSuperclass();

        if (superclass != null && superclass.isEnum()) {
            return superclass.getName();
        } else {
            return cls.getName();
        }
    }

    private String getTypeId(Class cls, Entity e, Embedded emb) {
        if (e != null) {
            // Annotation can be on super class - for the correct type Id we must check only the annotation on used Sub-Class.
            Entity clsEntity = annotationHelper.getAnnotationFromClass(cls, Entity.class);

            if (clsEntity != null && !clsEntity.typeId().equals(".")) {
                return clsEntity.typeId();
            }
        }

        if (emb != null) {
            // Annotation can be on super class - for the correct type Id we must check only the annotation on used Sub-Class.
            Embedded clsEmbedded = annotationHelper.getAnnotationFromClass(cls, Embedded.class);

            if (clsEmbedded != null && !clsEmbedded.typeId().equals(".")) {
                return clsEmbedded.typeId();
            }
        }

        return cls.getName();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> serializeMap(Map v, Type mapType) {
        Class<?> elementClass = null;
        Type elementType = null;

        if (mapType instanceof ParameterizedType) {
            elementClass = getElementClass((ParameterizedType) mapType);
            elementType = getElementType((ParameterizedType) mapType);
        }

        Map<String, Object> dbMap = new HashMap<>();

        for (Map.Entry<Object, Object> es : ((Map<Object, Object>) v).entrySet()) {
            Object k = es.getKey();

            if (!(k instanceof String)) {
                if (k instanceof Enum) {
                    k = ((Enum) k).name();
                } else {
                    log.debug("Map in Mongodb needs to have String as keys - using toString");
                    k = k.toString();

                    if (((String) k).contains(".")) {
                        log.warn(". not allowed as Key in Maps - converting to _");
                        k = ((String) k).replaceAll("\\.", "_");
                    }
                }
            }

            Object mval = es.getValue(); // ((Map) v).get(k);

            if (mval != null) {
                Class<?> mvalClass = mval.getClass();
                Entity mvalEntity = annotationHelper.getAnnotationFromHierarchy(mvalClass, Entity.class);
                Embedded mvalEmbedded = annotationHelper.getAnnotationFromHierarchy(mvalClass, Embedded.class);

                if (mvalEntity != null || mvalEmbedded != null) {
                    Map<String, Object> obj = serialize(mval);
                    obj.put("class_name", getTypeId(mvalClass, mvalEntity, mvalEmbedded));
                    mval = obj;
                } else if (mval instanceof Map) {
                    mval = serializeMap((Map) mval, elementType);
                } else if (mval instanceof Iterable) {
                    mval = serializeIterable((Iterable) mval, elementClass, elementType);
                } else if (mvalClass.isArray()) {
                    if (!mvalClass.getComponentType().equals(byte.class)) {
                        int arrayLength = Array.getLength(mval);
                        ArrayList lst = new ArrayList(arrayLength);

                        for (int i = 0; i < arrayLength; i++) {
                            lst.add(Array.get(mval, i));
                        }

                        mval = serializeIterable(lst, elementClass, elementType);
                    }
                } else if (mval instanceof Enum) {
                    mval = serializeEnum(elementClass, (Enum) mval);
                } else if (mval instanceof MorphiumId) {
                    mval = new ObjectId(((MorphiumId) mval).getBytes());
                } else if (!mvalClass.isPrimitive() && !mongoTypes.contains(mvalClass)) {
                    mval = serialize(mval);
                }
            }

            dbMap.put((String) k, mval);
        }

        return dbMap;
    }

    public Object serializeEnum(Class<?> declaredClass, Enum val) {
        if (declaredClass != null && Enum.class.isAssignableFrom(declaredClass)) {
            return val.name();
        } else {
            Map<String, Object> obj = new HashMap<>();
            obj.put("class_name", getTypeId(val));
            obj.put("name", val.name());
            return obj;
        }
    }

    @Override
    public <T> T deserialize(Class<? extends T> cls, String jsonString) throws RuntimeException {
        try {
            if (jsonString.startsWith("{")) {
                HashMap<String, Object> obj = (HashMap<String, Object>) jacksonOM.readValue(jsonString.getBytes(), Map.class); //jsonParser.parse(jsonString, containerFactory);
                return deserialize(cls, obj);
            } else {
                return (T)(jacksonOM.readValue(("{\"value\":" + jsonString + "}").getBytes(), Map.class).get("value"));
            }
        } catch (
                Exception e) {
            throw new RuntimeException("Parsing failed", e);
        }
    }

    @SuppressWarnings({"unchecked", "CastCanBeRemovedNarrowingVariableType", "CommentedOutCode"})
    @Override
    public <T> T deserialize(Class<? extends T> theClass, Map<String, Object> objectMap) {
        if (objectMap == null) {
            return null;
        }

        Class cls = theClass;

        if (customMappers.containsKey(cls)) {
            return (T) customMappers.get(cls).unmarshall(objectMap);
        }

        try {
            Entity entity = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class);
            Embedded embedded = annotationHelper.getAnnotationFromHierarchy(cls, Embedded.class);
            boolean objectIsEntity = entity != null || embedded != null;
            boolean warnOnNoEntitySerialization = morphium != null && morphium.getConfig() != null && morphium.getConfig().isWarnOnNoEntitySerialization();
            boolean objectSerializationEnabled = morphium == null || morphium.getConfig() == null || morphium.getConfig().isObjectSerializationEnabled();

            if (!warnOnNoEntitySerialization && objectSerializationEnabled && !objectIsEntity) {
                cls = BinarySerializedObject.class;
            }

            if (objectMap.get("class_name") != null || objectMap.get("className") != null) {
                //                if (log.isDebugEnabled()) {
                //                    log.debug("overriding cls - it's defined in dbObject");
                //                }
                try {
                    String cN = (String) objectMap.get("class_name");

                    if (cN == null) {
                        cN = (String) objectMap.get("className");
                    }

                    if (cN.equals("msg")) {
                        cN = Msg.class.getName();
                    }

                    cls = annotationHelper.getClassForTypeId(cN);
                } catch (ClassNotFoundException cnf) {
                    //cnf.printStackTrace();
                    log.warn("Class not found exception", cnf);
                    return null;
                }
            }

            if (cls.isEnum()) {
                return (T) Enum.valueOf((Class<? extends Enum>) cls, (String) objectMap.get("name"));
            }

            {
                Class<?> superclass = cls.getSuperclass();

                if (superclass != null && superclass.isEnum()) {
                    return (T) Enum.valueOf((Class<? extends Enum>) superclass, (String) objectMap.get("name"));
                }
            }

            Object ret = null;

            try {
                ret = cls.getDeclaredConstructor().newInstance();
            } catch (Exception ignored) {
            }

            if (ret == null) {
                final Constructor<Object> constructor;

                try {
                    constructor = (Constructor<Object>) reflection.newConstructorForSerialization(cls, Object.class.getDeclaredConstructor());
                    ret = constructor.newInstance();
                } catch (Exception e) {
                    log.error("Exception", e);
                }
            }

            if (ret == null) {
                throw new IllegalArgumentException("Could not instanciate " + cls.getName());
            }

            List<String> flds = annotationHelper.getFields(cls);

            for (String f : flds) {
                Object valueFromDb = objectMap.get(f);
                Field fld = annotationHelper.getField(cls, f);

                if (fld == null) {
                    continue;
                }

                Class<?> fldType = fld.getType();

                if (Modifier.isStatic(fld.getModifiers())) {
                    //skip static fields
                    continue;
                }

                if (customMappers.containsKey(fldType)) {
                    fld.set(ret, customMappers.get(fldType).unmarshall(valueFromDb));
                    continue;
                }

                if (fld.isAnnotationPresent(Aliases.class) && valueFromDb == null) {
                    Aliases al = fld.getAnnotation(Aliases.class);

                    for (String s : al.value()) {
                        if (objectMap.containsKey(s)) {
                            valueFromDb = objectMap.get(s);
                            break;
                        }
                    }
                }

                if (fld.isAnnotationPresent(AdditionalData.class)) {
                    //this field should store all data that is not put to fields
                    if (!Map.class.isAssignableFrom(fldType)) {
                        log.error("Could not deserialize additional data into fld of type " + fldType);
                        continue;
                    }

                    Map<String, Object> data = new HashMap<>();

                    for (Entry<String, Object> entry : objectMap.entrySet()) {
                        String k = entry.getKey();
                        Object v = entry.getValue();

                        if (flds.contains(k)) {
                            continue;
                        }

                        if (k.equals("_id")) {
                            //id already mapped
                            continue;
                        }

                        if (v instanceof Map) {
                            Map<String, Object> mapV = (Map<String, Object>) v;
                            String mapVClassName = (String) mapV.get("class_name");

                            if (mapVClassName != null) {
                                data.put(k, deserialize(annotationHelper.getClassForTypeId(mapVClassName), mapV));
                            } else {
                                data.put(k, deserializeMap(mapV));
                            }
                        } else if (v instanceof List && !((List) v).isEmpty() && ((List) v).get(0) instanceof Map) {
                            data.put(k, deserializeList((List<Object>) v));
                        } else {
                            data.put(k, v);
                        }
                    }

                    fld.set(ret, data);
                    continue;
                }

                if (fld.isAnnotationPresent(Encrypted.class)) {
                    if (valueFromDb == null) continue;

                    //encrypted field
                    Encrypted enc = fld.getAnnotation(Encrypted.class);
                    Class<? extends ValueEncryptionProvider> encCls = enc.provider();
                    ValueEncryptionProvider ep = encCls.newInstance();
                    String key = enc.keyName();

                    if (key.equals(".")) {
                        key = theClass.getName();
                    }

                    byte[] decKey = morphium.getEncryptionKeyProvider().getDecryptionKey(key);
                    ep.setDecryptionKey(decKey);

                    if (valueFromDb instanceof byte[]) {
                        valueFromDb = new String(ep.decrypt((byte[]) valueFromDb));
                    } else if (valueFromDb instanceof String) {
                        valueFromDb = new String(ep.decrypt(Base64.getDecoder().decode(valueFromDb.toString())));
                    } else {
                        throw new RuntimeException("Decryption not possible, value is no byte array or base64 string!");
                    }

                    try {
                        if (((String) valueFromDb).trim().startsWith("{") || ((String) valueFromDb).trim().startsWith("[")) {
                            valueFromDb = deserialize(fldType, (String) valueFromDb);
                        } else if (valueFromDb instanceof Map) {
                            valueFromDb = deserialize(Map.class, (String) valueFromDb).get("value");
                        }
                    } catch (Exception e) {
                        log.debug("Not a json string, cannot deserialize further");
                    }

                    annotationHelper.setValue(ret, valueFromDb, f);
                    continue;
                }

                if (valueFromDb == null) {
                    if (!fldType.isPrimitive() && objectMap.containsKey(f)) {
                        fld.set(ret, null);
                    }

                    continue;
                }

                Object value = null;

                if (!Collection.class.isAssignableFrom(fldType) && fld.isAnnotationPresent(Reference.class)) {
                    //A reference - only id stored
                    Reference reference = fld.getAnnotation(Reference.class);
                    MorphiumReference r = null;

                    if (morphium == null) {
                        log.error("Morphium not set - could not de-reference!");
                    } else {
                        if (Map.class.isAssignableFrom(fldType)) {
                            Map<Object, Object> v = new HashMap<>();

                            //Reference map
                            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) valueFromDb).entrySet()) {
                                Object id;

                                if (!(e.getValue() instanceof Map)) {
                                    id = e.getValue();
                                    r = null;
                                } else {
                                    Map<String, Object> ref = (Map<String, Object>) e.getValue();
                                    r = deserialize(MorphiumReference.class, ref);
                                    id = r.getId();
                                }

                                String collectionName = null;
                                Class type = fldType;

                                if (r != null) {
                                    if (r.getCollectionName() != null) {
                                        collectionName = r.getCollectionName();
                                    } else {
                                        collectionName = getCollectionName(annotationHelper.getClassForTypeId(r.getClassName()));
                                    }

                                    type = annotationHelper.getClassForTypeId(r.getClassName());
                                } else {
                                    if (annotationHelper.isAnnotationPresentInHierarchy(fldType, Entity.class)) {
                                        collectionName = getCollectionName(fldType);
                                    }
                                }

                                if (collectionName == null) {
                                    throw new IllegalArgumentException("Could not create reference!");
                                }

                                if (reference.lazyLoading()) {
                                    List<String> lst = annotationHelper.getFields(fldType, Id.class);

                                    if (lst.isEmpty()) {
                                        throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                                    }

                                    if (id instanceof String && annotationHelper.getField(fldType, lst.get(0)).getType().equals(MorphiumId.class)) {
                                        id = new MorphiumId(id.toString());
                                    } else if (id instanceof ObjectId && annotationHelper.getField(fldType, lst.get(0)).getType().equals(MorphiumId.class)) {
                                        id = new MorphiumId(((ObjectId) id).toByteArray());
                                    }

                                    value = morphium.createLazyLoadedEntity(fldType, id, collectionName);
                                } else {
                                    try {
                                        value = morphium.findById(type, id, collectionName);
                                    } catch (MorphiumAccessVetoException ex) {
                                        log.debug("not dereferencing due to veto from listener", ex);
                                    }
                                }

                                v.put(e.getKey(), value);
                            }

                            value = v;
                        } else {
                            Object id;

                            if (!(valueFromDb instanceof Map)) {
                                id = valueFromDb;
                            } else {
                                Map<String, Object> ref = (Map<String, Object>) valueFromDb;
                                r = deserialize(MorphiumReference.class, ref);
                                id = r.getId();
                            }

                            String collection = getCollectionName(fldType);

                            if (r != null && r.getCollectionName() != null) {
                                collection = r.getCollectionName();
                            }

                            if (id != null) {
                                if (reference.lazyLoading()) {
                                    List<String> lst = annotationHelper.getFields(fldType, Id.class);

                                    if (lst.isEmpty()) {
                                        throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                                    }

                                    if (id instanceof String && annotationHelper.getField(fldType, lst.get(0)).getType().equals(MorphiumId.class)) {
                                        id = new MorphiumId(id.toString());
                                    }

                                    value = morphium.createLazyLoadedEntity(fldType, id, collection);
                                } else {
                                    //                                Query q = morphium.createQueryFor(fld.getSearchType());
                                    //                                q.f("_id").eq(id);
                                    try {
                                        value = morphium.findById(fldType, id, collection);
                                    } catch (MorphiumAccessVetoException e) {
                                        log.debug("not dereferencing due to veto from listener", e);
                                    }
                                }
                            } else {
                                value = null;
                            }
                        }
                    }
                } else if (fld.isAnnotationPresent(Id.class)) {
                    value = objectMap.get("_id");

                    if (value != null && !value.getClass().equals(fldType)) {
                        log.debug("read value and field type differ...");

                        if (fldType.equals(MorphiumId.class)) {
                            log.debug("trying objectID conversion");

                            if (value.getClass().equals(String.class)) {
                                try {
                                    value = new MorphiumId((String) value);
                                } catch (Exception e) {
                                    log.error("Value and field type differ - Id conversion failed - setting returning null", e);
                                    return null;
                                }
                            }
                        } else if (value.getClass().equals(MorphiumId.class)) {
                            if (fldType.equals(String.class)) {
                                value = value.toString();
                            } else if (fldType.equals(Long.class) || fldType.equals(long.class)) {
                                value = ((MorphiumId) value).getTime();
                            } else {
                                log.error("cannot convert - ID IS SET TO NULL. Type read from db is " + value.getClass().getName() + " - expected value is " + fldType.getName());
                                return null;
                            }
                        }
                    }
                } else if (annotationHelper.isAnnotationPresentInHierarchy(fldType, Entity.class) || annotationHelper.isAnnotationPresentInHierarchy(fldType, Embedded.class)) {
                    //entity! embedded
                    value = deserialize(fldType, (Map<String, Object>) valueFromDb);
                    //                    List lst = new ArrayList<Object>();
                    //                    lst.add(value);
                    //                    morphium.firePostLoad(lst);
                } else if (Map.class.isAssignableFrom(fldType) && valueFromDb instanceof Map) {
                    value = fillMap(fld.getGenericType(), (Map<String, Object>) valueFromDb);
                } else if (Collection.class.isAssignableFrom(fldType) || fldType.isArray()) {
                    List<?> collection = null;

                    if ((valueFromDb instanceof Binary)) {
                        valueFromDb = Collections.singletonList(((Binary) valueFromDb).getData());
                    }

                    if (valueFromDb.getClass().isArray()) {
                        List lst = new ArrayList();

                        //a real array!
                        if (valueFromDb.getClass().getComponentType().isPrimitive()) {
                            if (valueFromDb.getClass().getComponentType().equals(int.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject((int[]) valueFromDb));
                            } else if (valueFromDb.getClass().getComponentType().equals(double.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject(((double[]) valueFromDb)));
                            } else if (valueFromDb.getClass().getComponentType().equals(float.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject(((float[]) valueFromDb)));
                            } else if (valueFromDb.getClass().getComponentType().equals(boolean.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject(((boolean[]) valueFromDb)));
                            } else if (valueFromDb.getClass().getComponentType().equals(byte.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject(((byte[]) valueFromDb)));
                            } else if (valueFromDb.getClass().getComponentType().equals(char.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject(((char[]) valueFromDb)));
                            } else if (valueFromDb.getClass().getComponentType().equals(long.class)) {
                                lst = Arrays.asList(ArrayUtils.toObject(((long[]) valueFromDb)));
                            }
                        } else {
                            Collections.addAll(lst, (Object[]) valueFromDb);
                        }

                        collection = lst;
                    } else {
                        collection = (List<?>) valueFromDb;
                    }

                    value = fillCollection(fld.getAnnotation(Reference.class), fldType, fld.getGenericType(), collection);
                } else {
                    Class<?> superclass = fldType.getSuperclass();

                    if (fldType.isEnum()) {
                        value = Enum.valueOf((Class<? extends Enum>) fldType, (String) valueFromDb);
                    } else if (superclass != null && superclass.isEnum()) {
                        value = Enum.valueOf((Class<? extends Enum>) superclass, (String) valueFromDb);
                    } else if (valueFromDb instanceof ObjectId) {
                        if (fldType.equals(MorphiumId.class)) {
                            if (valueFromDb instanceof ObjectId) {
                                value = new MorphiumId(((ObjectId) valueFromDb).toHexString());
                            } else if (valueFromDb instanceof String) {
                                value = new MorphiumId((String) valueFromDb);
                            } else {
                                log.error("Could not deserialize Value from DB of type " + valueFromDb.getClass().getName() + " and set it to morphiumId");
                            }
                        } else {
                            //assuming object
                            value = new MorphiumId(((ObjectId) valueFromDb).toByteArray());
                        }
                    } else if (Serializable.class.isAssignableFrom(fldType) && !mongoTypes.contains(fldType) && (valueFromDb instanceof Map)) {
                        value = deserialize(BinarySerializedObject.class, (Map) valueFromDb);
                    } else {
                        value = valueFromDb;
                    }
                }

                annotationHelper.setValue(ret, value, f);
            }

            if (entity != null) {
                flds = annotationHelper.getFields(cls, Id.class);

                if (flds.isEmpty()) {
                    throw new RuntimeException("Error - class does not have an ID field!");
                }

                Field field = annotationHelper.getField(cls, flds.get(0));
                Class<?> fieldType = field.getType();
                Object idValue = objectMap.get("_id");

                if (idValue != null) { // Embedded entitiy?
                    Class<?> idValueClass = idValue.getClass();

                    if (idValueClass.equals(fieldType) || fieldType.isAssignableFrom(idValueClass)) {
                        field.set(ret, idValue);
                    } else if (fieldType.equals(String.class) && idValueClass.equals(MorphiumId.class)) {
                        log.warn("ID type missmatch - field is string but got objectId from mongo - converting");
                        field.set(ret, idValue.toString());
                    } else if (fieldType.equals(MorphiumId.class) && idValueClass.equals(ObjectId.class)) {
                        field.set(ret, new MorphiumId(((ObjectId) idValue).toByteArray()));
                    } else if (fieldType.equals(ObjectId.class) && idValueClass.equals(MorphiumId.class)) {
                        field.set(ret, new ObjectId(((MorphiumId) idValue).getBytes()));
                    } else if (fieldType.equals(ObjectId.class) && idValueClass.equals(String.class)) {
                        field.set(ret, new ObjectId(((ObjectId) idValue).toString()));
                    } else if (fieldType.equals(MorphiumId.class) && idValueClass.equals(String.class)) {
                        // log.warn("ID type missmatch - field is objectId but got string from db - trying conversion");
                        field.set(ret, new MorphiumId((String) idValue));
                    } else if (fieldType.equals(UUID.class) && idValueClass.equals(byte[].class)) {
                        // Convert Java Legacy UUID from byte array to object
                        ByteBuffer bb = ByteBuffer.wrap((byte[]) idValue);
                        field.set(ret, new UUID(bb.getLong(), bb.getLong()));
                        // The following cases are more commen with Aggregation
                    } else if (fieldType.equals(int.class) && idValueClass.equals(Integer.class)) {
                        field.set(ret, (Integer) idValue);
                    } else if (fieldType.equals(long.class) && idValueClass.equals(Long.class)) {
                        field.set(ret, (Long) idValue);
                    } else if (fieldType.equals(double.class) && idValueClass.equals(Double.class)) {
                        field.set(ret, (Double) idValue);
                    } else if (fieldType.equals(float.class) && idValueClass.equals(Float.class)) {
                        field.set(ret, (Float) idValue);
                    } else if (fieldType.equals(boolean.class) && idValueClass.equals(Boolean.class)) {
                        field.set(ret, (Boolean) idValue);
                    } else {
                        log.error("ID type missmatch");
                        throw new IllegalArgumentException("ID type missmatch. Field in '" + ret.getClass().toString() + "' is '" + fieldType + "' but we got '" + idValueClass + "' from Mongo!");
                    }
                }
            }

            if (ret instanceof BinarySerializedObject) {
                BinarySerializedObject bso = (BinarySerializedObject) ret;
                Decoder dec = Base64.getMimeDecoder();
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(dec.decode(bso.getB64Data())));
                return (T) in.readObject();
            }

            return (T) ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //recursively fill class
    }

    public Object fillArray(Class<?> componentType, Collection<?> c) {
        Object arr = Array.newInstance(componentType, c.size());

        if (int.class.equals(componentType) || Integer.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Integer) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).intValue());
                }
            }
        } else if (long.class.equals(componentType) || Long.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Long) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).longValue());
                }
            }
        } else if (float.class.equals(componentType) || Float.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Float) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).floatValue());
                }
            }
        } else if (double.class.equals(componentType) || Double.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Double) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).doubleValue());
                }
            }
        } else if (byte.class.equals(componentType) || Byte.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Byte) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).byteValue());
                }
            }
        } else if (short.class.equals(componentType) || Short.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Short) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).shortValue());
                }
            }
        } else if (boolean.class.equals(componentType) || Boolean.class.equals(componentType)) {
            int i = 0;

            for (Object o : c) {
                if (o instanceof Boolean) {
                    Array.set(arr, i++, o);
                } else if (o instanceof Number) {
                    Array.set(arr, i++, ((Number) o).intValue() != 0);
                } else {
                    Array.set(arr, i++, Boolean.valueOf(o.toString()));
                }
            }
        } else {
            int i = 0;

            for (Object o : c) {
                Array.set(arr, i++, o);
            }
        }

        return arr;
    }

    public Map<String, Object> deserializeMap(Map<String, Object> dbObject) {
        Map<String, Object> retMap = new HashMap<>(dbObject);

        if (dbObject != null) {
            for (Entry<String, Object> entry : dbObject.entrySet()) {
                retMap.put(entry.getKey(), unmarshallInternal(entry.getValue()));
            }
        } else {
            retMap = null;
        }

        return retMap;
    }

    private Object unmarshallInternal(Object val) {
        if (val instanceof Map) {
            Map<String, Object> mapVal = (Map<String, Object>) val;
            String cn = (String) mapVal.get("class_name");

            if (cn == null) {
                cn = (String) mapVal.get("className");
            }

            if (cn != null) {
                //Entity to map!
                try {
                    Class ecls = annotationHelper.getClassForTypeId(cn);
                    Object obj = deserialize(ecls, mapVal);

                    if (obj != null) {
                        return obj;
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else {
                String d = (String) mapVal.get("_b64data");

                if (d == null) {
                    d = (String) mapVal.get("b64Data");
                }

                if (d != null) {
                    Decoder dec = Base64.getMimeDecoder();
                    ObjectInputStream in;

                    try {
                        in = new ObjectInputStream(new ByteArrayInputStream(dec.decode(d)));
                        return in.readObject();
                    } catch (IOException | ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // maybe a normal map --> recurse
                    return deserializeMap(mapVal);
                }
            }
        } else if (val instanceof ObjectId) {
            val = new MorphiumId(((ObjectId) val).toByteArray());
        } else if (val instanceof List) {
            List<Object> lst = (List<Object>) val;
            return deserializeList(lst);
        }

        return val;
    }

    public List deserializeList(List<Object> lst) {
        return lst.stream().map(this::unmarshallInternal).collect(Collectors.toList());
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private void fillCollection(Reference ref, Type listType, Class elementClass, Type elementType, List<?> fromDB, Collection toFillIn) {
        if (ref != null) {
            for (Object obj : fromDB) {
                if (obj == null) {
                    toFillIn.add(null);
                    continue;
                }

                MorphiumReference r = deserialize(MorphiumReference.class, (Map<String, Object>) obj);
                Class type;

                try {
                    type = annotationHelper.getClassForTypeId(r.getClassName());
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }

                if (r.getCollectionName() == null) {
                    r.setCollectionName(getCollectionName(type));
                }

                if (ref.lazyLoading()) {
                    if (r.getId() instanceof String && morphium.getARHelper().getIdField(type).getType().equals(MorphiumId.class)) {
                        r.setId(new MorphiumId(r.getId().toString()));
                    }

                    toFillIn.add(morphium.createLazyLoadedEntity(type, r.getId(), r.getCollectionName()));
                } else {
                    toFillIn.add(morphium.findById(type, r.getId(), r.getCollectionName()));
                }
            }

            return;
        }

        for (Object val : new ArrayList<>(fromDB)) {
            if (val instanceof Map) {
                //Override type if className is specified - needed for polymoprh lists etc.
                if (((Map<String, Object>) val).containsKey("class_name") || ((Map<String, Object>) val).containsKey("className")) {
                    //Entity to map!
                    String cn = (String)((Map<String, Object>) val).get("class_name");

                    if (cn == null) {
                        cn = (String)((Map<String, Object>) val).get("className");
                    }

                    try {
                        Class ecls = annotationHelper.getClassForTypeId(cn);
                        Object um = deserialize(ecls, (Map<String, Object>) val);

                        if (um != null) {
                            toFillIn.add(um);
                        }
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Could not find class", e);
                    }

                    continue;
                }

                if (listType != null) {
                    //have a list of something
                    if (Map.class.isAssignableFrom(elementClass)) {
                        // that is an actual map!
                        toFillIn.add(fillMap((ParameterizedType) elementType, (Map<String, Object>) val));
                        continue;
                    } else {
                        Entity entity = annotationHelper.getAnnotationFromHierarchy(elementClass, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
                        Embedded embedded = annotationHelper.getAnnotationFromHierarchy(elementClass, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);

                        if (entity != null || embedded != null) {
                            toFillIn.add(deserialize(elementClass, (Map<String, Object>) val));
                            continue;
                        }
                    }
                } else {
                    log.warn("Cannot de-reference to unknown collection type - trying object instead");
                    toFillIn.add(val);
                    continue;
                }
            } else if (val instanceof List) {
                toFillIn.add(fillCollection(null, elementClass, elementType, (List<?>) val));
                continue;
            }

            Object unmarshalled = unmarshallInternal(val);
            elementClass = ClassUtils.primitiveToWrapper(elementClass);

            if (unmarshalled != null && elementClass != null && !elementClass.isAssignableFrom(unmarshalled.getClass())) {
                try {
                    unmarshalled = AnnotationAndReflectionHelper.convertType(unmarshalled, "", elementClass);
                } catch (Exception e) {
                    // log.warn("", e);
                }
            }

            toFillIn.add(unmarshalled);
        }
    }

    public Object fillCollection(Reference ref, Class<?> collectionClass, Type collectionType, List<?> fromDb) {
        if (collectionClass == null) {
            collectionClass = Object.class;
        }

        if (collectionType == null) {
            collectionType = Object.class;
        }

        Class elementClass = null;
        Type elementType = null;

        if (collectionType instanceof ParameterizedType) {
            elementClass = getElementClass((ParameterizedType) collectionType);
            elementType = getElementType((ParameterizedType) collectionType);
        } else if (collectionType instanceof GenericArrayType) {
            elementType = ((GenericArrayType) collectionType).getGenericComponentType();

            if (elementType instanceof Class) {
                elementClass = (Class) elementType;
            } else if (elementType instanceof ParameterizedType) {
                Type rawType = ((ParameterizedType) elementType).getRawType();

                if (rawType instanceof Class) {
                    elementClass = (Class) rawType;
                }
            }
        }

        Class<?> componentType = collectionClass.getComponentType();

        if (componentType != null && elementClass == null) {
            elementClass = componentType;
        }

        if (elementClass == null) {
            elementClass = Object.class;
        }

        Collection innerCollection = null;

        if (List.class.isAssignableFrom(collectionClass)) {
            innerCollection = new ArrayList(fromDb.size());
        } else if (Enum.class.isAssignableFrom(elementClass) && Collection.class.isAssignableFrom(collectionClass)) {
            innerCollection = EnumSet.noneOf(elementClass);
        } else if (Set.class.isAssignableFrom(collectionClass)) {
            innerCollection = new LinkedHashSet<>();
        } else {
            innerCollection = new ArrayList(fromDb.size());
        }

        fillCollection(ref, collectionType, elementClass, elementType, fromDb, innerCollection);

        if (componentType != null) {
            return fillArray(componentType, innerCollection);
        } else {
            return innerCollection;
        }
    }

    public static Type getElementType(ParameterizedType parameterizedType) {
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();

        if (actualTypeArguments != null && actualTypeArguments.length > 0) {
            return actualTypeArguments[actualTypeArguments.length - 1];
        }

        return null;
    }

    private Class getElementClass(ParameterizedType parameterizedType) {
        Type[] parameters = parameterizedType.getActualTypeArguments();
        Type relevantParameter = parameters[parameters.length - 1];

        if (relevantParameter instanceof Class) {
            return (Class) relevantParameter;
        }

        if (relevantParameter instanceof ParameterizedType) {
            ParameterizedType parameterType = (ParameterizedType) relevantParameter;

            if (parameterType.getRawType() instanceof Class) {
                return (Class) parameterType.getRawType();
            } else {
                try {
                    return annotationHelper.getClassForTypeId(parameterType.getTypeName());
                } catch (ClassNotFoundException e) {
                    log.error("Could not determin class for type " + parameterType.getRawType().getTypeName());
                    return Object.class;
                }
            }
        } else if (relevantParameter instanceof WildcardType) {
            return ((WildcardType) relevantParameter).getClass();
        } else {
            log.error("Could not determin type of element!");
            return Object.class;
        }
    }

    private Class getKeyClass(ParameterizedType parameterizedType) {
        Type[] parameters = parameterizedType.getActualTypeArguments();
        Type relevantParameter = parameters[0];

        if (relevantParameter instanceof Class) {
            return (Class) relevantParameter;
        }

        if (relevantParameter instanceof ParameterizedType) {
            ParameterizedType parameterType = (ParameterizedType) relevantParameter;

            if (parameterType.getRawType() instanceof Class) {
                return (Class) parameterType.getRawType();
            } else {
                try {
                    return annotationHelper.getClassForTypeId(parameterType.getTypeName());
                } catch (ClassNotFoundException e) {
                    log.error("Could not determin class for type " + parameterType.getRawType().getTypeName());
                    return Object.class;
                }
            }
        } else if (relevantParameter instanceof WildcardType) {
            return ((WildcardType) relevantParameter).getClass();
        } else {
            log.error("Could not determin type of key!");
            return Object.class;
        }
    }

    @SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
    protected Map fillMap(Type mapType, Map<String, Object> fromDB) {
        boolean useEnumMap = false;
        Class<?> keyClass = null;

        if (mapType instanceof ParameterizedType) {
            ParameterizedType genericType = (ParameterizedType) mapType;
            keyClass = getKeyClass(genericType);

            if (EnumMap.class.isAssignableFrom((Class<?>) genericType.getRawType())) {
                useEnumMap = true;
            } else {
                if (Enum.class.isAssignableFrom(keyClass)) {
                    useEnumMap = true;
                }
            }
        }

        Map toFill;

        if (useEnumMap) {
            toFill = new EnumMap<>((Class<? extends Enum>) keyClass);
        } else {
            toFill = new HashMap();
        }

        if (fromDB != null && !fromDB.isEmpty()) {
            fillMap(mapType, fromDB, toFill);
        }

        return toFill;
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    protected void fillMap(Type mapType, Map<String, Object> fromDB, Map toFillIn) {
        Class keyClass = null;
        Class elementClass = null;
        Type elementType = null;

        if (mapType instanceof ParameterizedType) {
            keyClass = getKeyClass((ParameterizedType) mapType);
            elementClass = getElementClass((ParameterizedType) mapType);
            elementType = getElementType((ParameterizedType) mapType);
        }

        Method convertMethod = null;

        if (keyClass != null && !String.class.equals(keyClass)) {
            convertMethod = AnnotationAndReflectionHelper.getConvertMethod(keyClass);
        }

        for (Entry<String, Object> entry : fromDB.entrySet()) {
            String stringKey = entry.getKey();
            Object key = stringKey;

            if (convertMethod != null) {
                try {
                    key = convertMethod.invoke(null, stringKey);
                } catch (ReflectiveOperationException | RuntimeException e) {
                    log.error("Could not convert " + stringKey + " to key type " + keyClass, e);
                    continue;
                }
            }

            Object val = entry.getValue();

            if (val instanceof Map) {
                if (elementClass != null) {
                    //have a list of something
                    if (Map.class.isAssignableFrom(elementClass)) {
                        // this is an actual map
                        toFillIn.put(key, fillMap(elementType, (Map<String, Object>) val));
                        continue;
                    } else {
                        Entity entity = annotationHelper.getAnnotationFromHierarchy(elementClass, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
                        Embedded embedded = annotationHelper.getAnnotationFromHierarchy(elementClass, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);

                        if (entity != null || embedded != null) {
                            toFillIn.put(key, deserialize(elementClass, (Map<String, Object>) val));
                            continue;
                        }
                    }
                }
            } else if (val instanceof List) {
                //list in list
                toFillIn.put(key, fillCollection(null, elementClass, elementType, (List<?>) val));
                continue;
            }

            Object unmarshalled = unmarshallInternal(val);

            if (unmarshalled != null && elementClass != null && !elementClass.isAssignableFrom(unmarshalled.getClass())) {
                try {
                    unmarshalled = AnnotationAndReflectionHelper.convertType(unmarshalled, "", elementClass);
                } catch (Exception e) {
                    log.warn("", e);
                }
            }

            toFillIn.put(key, unmarshalled);
        }
    }

    @Override
    public Class<?> getClassForCollectionName(String collectionName) {
        return classByCollectionName.get(collectionName);
    }
}
