package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.encryption.ValueEncryptionProvider;
import de.caluga.morphium.mapping.BigIntegerTypeMapper;
import de.caluga.morphium.mapping.MorphiumTypeMapper;
import org.bson.types.ObjectId;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.ReflectionFactory;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigInteger;
import java.util.*;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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
    private final JSONParser jsonParser = new JSONParser();
    private final List<Class<?>> mongoTypes;
    private final ContainerFactory containerFactory;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper(true);
    private final Map<Class<?>, MorphiumTypeMapper> customMappers = new ConcurrentHashMap<>();
    private Morphium morphium;

    public ObjectMapperImpl() {

        nameProviders = new ConcurrentHashMap<>();
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

        customMappers.put(BigInteger.class, new BigIntegerTypeMapper());

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

        if (nameProviders.get(cls) == null) {
            NameProvider np = p.nameProvider().getDeclaredConstructor().newInstance();
            setNameProviderForClass(cls, np);
        }
        return nameProviders.get(cls);
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
        } catch (IllegalAccessException|NoSuchMethodException|InvocationTargetException e) {
            log.error("Illegal Access during instanciation of NameProvider: " + p.nameProvider().getName(), e);
            throw new RuntimeException("Illegal Access during instanciation", e);
        }
    }


    public Object marshallIfNecessary(Object o) {
        if (o == null) {
            return null;
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
            ArrayList lst = new ArrayList();
            for (int i = 0; i < Array.getLength(o); i++) {
                lst.add(marshallIfNecessary(Array.get(o, i)));
            }
            return serializeList(lst);
        }
        if (Collection.class.isAssignableFrom(o.getClass())) {
            ArrayList lst = new ArrayList((Collection) o);
            return serializeList(lst);
        }
        if (Map.class.isAssignableFrom(o.getClass())) {
            return serializeMap((Map) o);
        }
//        if (o instanceof MorphiumId) {
//            o = new ObjectId(((MorphiumId) o).getBytes());
//        }
        return o;
    }


    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> serialize(Object o) {
        if (o==null) return new HashMap<>();
        Class c = annotationHelper.getRealClass(o.getClass());
        if (customMappers.containsKey(c)) {
            Object ret = customMappers.get(c).marshall(o);
            if (ret instanceof Map) {
                ((Map) ret).put("class_name", o.getClass().getName());
                return (Map<String, Object>) ret;
            } else {
                return Utils.getMap("value", ret);
            }
        }
        //recursively map object to mongo-Object...
        if (!annotationHelper.isEntity(o) && !morphium.getConfig().isWarnOnNoEntitySerialization()) {
            if (morphium == null || morphium.getConfig().isObjectSerializationEnabled()) {
                if (o instanceof Serializable) {
                    try {
                        BinarySerializedObject obj = new BinarySerializedObject();
                        obj.setOriginalClassName(o.getClass().getName());
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        ObjectOutputStream oout = new ObjectOutputStream(out);
                        oout.writeObject(o);
                        oout.flush();

                        Encoder enc = Base64.getMimeEncoder();

                        String str = new String(enc.encode(out.toByteArray()));
                        obj.setB64Data(str);
                        return serialize(obj);

                    } catch (IOException e) {
                        throw new IllegalArgumentException("Binary serialization failed! " + o.getClass().getName(), e);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot write object to db that is neither entity, embedded nor serializable! ObjectType: " + o.getClass().getName());
                }
            }
            throw new IllegalArgumentException("Object is no entity: " + o.getClass().getSimpleName());
        }
        if (!annotationHelper.isEntity(o) && morphium.getConfig().isWarnOnNoEntitySerialization()) {
            log.warn("Serializing non-entity of type " + o.getClass().getName());
        }

        HashMap<String, Object> dbo = new HashMap<>();
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
        String cn = cls.getName();
        if (e != null && !e.typeId().equals(".")) cn = e.typeId();
        if (emb != null && !emb.typeId().equals(".")) cn = emb.typeId();
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
                if (fld.isAnnotationPresent(Encrypted.class)) {
                    try {
                        Encrypted enc = fld.getAnnotation(Encrypted.class);
                        ValueEncryptionProvider encP = enc.provider().getDeclaredConstructor().newInstance();
                        byte[] encKey = morphium.getEncryptionKeyProvider().getEncryptionKey(enc.keyName());
                        encP.setEncryptionKey(encKey);
                        byte[] encrypted = encP.encrypt(Utils.toJsonString(marshallIfNecessary(fld.get(o))).getBytes());
                        dbo.put(fName, encrypted);
                        continue;
                    } catch (Exception exc) {
                        throw new RuntimeException("Ecryption failed. Field: " + fName + " class: " + o.getClass().getName(), exc);
                    }
                }
                AdditionalData ad = fld.getAnnotation(AdditionalData.class);
                if (ad != null) {
                    if (!ad.readOnly()) {
                        //storing additional data
                        if (fld.get(o) != null) {
                            dbo.putAll((Map) serializeMap((Map<String, Object>) fld.get(o)));
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
                Object value = fld.get(o);
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
                                if (rec != null) //noinspection DuplicatedCode
                                {
                                    Object id = annotationHelper.getId(rec);
                                    if (id == null) {
                                        id = automaticStore(r, rec);
                                    }
                                    if (morphium == null) {
                                        throw new RuntimeException("cannot set dbRef - morphium is not set");
                                    }
                                    MorphiumReference ref = new MorphiumReference(annotationHelper.getRealClass(rec.getClass()).getName(), id);

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
                                MorphiumReference ref = new MorphiumReference(annotationHelper.getRealClass(rec.getClass()).getName(), id);
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
                                v = serializeMap((Map) v);
                            } else if (v.getClass().isArray()) {
                                if (!v.getClass().getComponentType().equals(byte.class)) {
                                    List lst = new ArrayList<>();
                                    for (int i = 0; i < Array.getLength(v); i++) {
                                        lst.add(marshallIfNecessary(Array.get(v, i)));
                                    }
                                    v = serializeList(lst);
                                }
                            } else if (v instanceof List) {
                                v = serializeList((List) v);
                            } else if (v instanceof Iterable) {
                                ArrayList lst = new ArrayList();
                                for (Object i : (Iterable) v) {
                                    lst.add(i);
                                }
                                v = serializeList(lst);
                            } else if (v.getClass().equals(GregorianCalendar.class)) {
                                v = ((GregorianCalendar) v).getTime();
                            } else if (v.getClass().equals(MorphiumId.class)) {
                                v = new ObjectId(((MorphiumId) v).getBytes());
                            } else if (customMappers.containsKey(v.getClass())) {
                                v = customMappers.get(v.getClass()).marshall(v);
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

    public List<Object> serializeList(List v) {
        List<Object> lst = new ArrayList<>();
        for (Object lo : v) {
            if (lo != null) {
                if (annotationHelper.isAnnotationPresentInHierarchy(lo.getClass(), Entity.class) ||
                        annotationHelper.isAnnotationPresentInHierarchy(lo.getClass(), Embedded.class)) {
                    Map<String, Object> marshall = serialize(lo);
                    String cn = getTypeId(lo);
                    marshall.put("class_name", cn);
                    lst.add(marshall);
                } else if (lo instanceof List) {
                    lst.add(serializeList((List) lo));
                } else if (lo instanceof Map) {
                    lst.add(serializeMap(((Map) lo)));
                } else if (lo instanceof MorphiumId) {
                    lst.add(new ObjectId(((MorphiumId) lo).getBytes()));
                } else if (lo.getClass().isEnum()) {
                    Map<String, Object> obj = new HashMap<>();
                    obj.put("class_name", getTypeId(lo));
                    obj.put("name", ((Enum) lo).name());
                    lst.add(obj);
                    //throw new IllegalArgumentException("List of enums not supported yet");
                } else if (lo.getClass().isPrimitive()
                        || mongoTypes.contains(lo.getClass())) {
                    lst.add(lo);
                } else if (lo.getClass().isArray()) {
                    if (lo.getClass().getComponentType().equals(byte.class)) {
                        lst.add(lo);
                    } else {
                        for (int i = 0; i < Array.getLength(lo); i++) {
                            try {
                                lst.add(marshallIfNecessary(Array.get(lo, i)));
                            } catch (Exception e) {
                                lst.add(marshallIfNecessary(((Integer) Array.get(lo, i)).byteValue()));
                            }
                        }
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

    private String getTypeId(Object lo) {
        String cn = lo.getClass().getName();
        Entity e = annotationHelper.getAnnotationFromHierarchy(lo.getClass(), Entity.class);
        Embedded emb = annotationHelper.getAnnotationFromHierarchy(log.getClass(), Embedded.class);
        if (e != null && !e.typeId().equals(".")) {
            cn = e.typeId();
        } else if (emb != null && !emb.typeId().equals(".")) {
            cn = emb.typeId();
        }
        return cn;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> serializeMap(Map v) {
        Map<String, Object> dbMap = new HashMap<>();
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
                    Map<String, Object> obj = serialize(mval);
                    obj.put("class_name", getTypeId(mval));
                    mval = obj;
                } else if (mval instanceof Map) {
                    mval = serializeMap((Map) mval);
                } else if (mval instanceof List) {
                    mval = serializeList((List) mval);
                } else if (mval.getClass().isArray()) {
                    if (!mval.getClass().getComponentType().equals(byte.class)) {
                        ArrayList lst = new ArrayList();
                        for (int i = 0; i < Array.getLength(mval); i++) {
                            lst.add(marshallIfNecessary(Array.get(mval, i)));
                        }
                        mval = serializeList(lst);
                    }
                } else if (mval.getClass().isEnum()) {
                    Map<String, Object> obj = new HashMap<>();
                    obj.put("class_name", getTypeId(mval));
                    obj.put("name", ((Enum) mval).name());
                } else if (mval instanceof MorphiumId) {
                    mval = new ObjectId(((MorphiumId) mval).getBytes());
                } else if (!mval.getClass().isPrimitive() && !mongoTypes.contains(mval.getClass())) {
                    mval = serialize(mval);
                }
            }
            dbMap.put((String) k, mval);
        }
        return dbMap;
    }

    @Override
    public <T> T deserialize(Class<? extends T> cls, String jsonString) throws ParseException {
        if (jsonString.startsWith("{")) {
            HashMap<String, Object> obj = (HashMap<String, Object>) jsonParser.parse(jsonString, containerFactory);
            return deserialize(cls, obj);
        } else {
            return (T) ((HashMap<String, Object>) jsonParser.parse("{\"value\":" + jsonString + "}", containerFactory)).get("value");
        }


    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(Class<? extends T> theClass, Map<String, Object> o) {
        if (o == null) {
            return null;
        }
        Class cls = theClass;
        if (customMappers.containsKey(cls)) {
            return (T) customMappers.get(cls).unmarshall(o);
        }
        try {
            if (morphium != null && !morphium.getConfig().isWarnOnNoEntitySerialization() && morphium.getConfig().isObjectSerializationEnabled() && !annotationHelper.isAnnotationPresentInHierarchy(cls, Entity.class) && !(annotationHelper.isAnnotationPresentInHierarchy(cls, Embedded.class))) {
                cls = BinarySerializedObject.class;
            }
            if (o.get("class_name") != null || o.get("className") != null) {
                //                if (log.isDebugEnabled()) {
                //                    log.debug("overriding cls - it's defined in dbObject");
                //                }
                try {
                    String cN = (String) o.get("class_name");
                    if (cN == null) {
                        cN = (String) o.get("className");
                    }
                    cls = annotationHelper.getClassForTypeId(cN);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            if (cls.isEnum()) {
                T[] en = (T[]) cls.getEnumConstants();
                for (Enum e : ((Enum[]) en)) {
                    if (e.name().equals(o.get("name"))) {
                        return (T) e;
                    }
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
                    constructor = (Constructor<Object>) reflection.newConstructorForSerialization(
                            cls, Object.class.getDeclaredConstructor());
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

                Object valueFromDb = o.get(f);
                Field fld = annotationHelper.getField(cls, f);
                if (Modifier.isStatic(fld.getModifiers())) {
                    //skip static fields
                    continue;
                }
                if (customMappers.containsKey(fld.getType())) {
                    fld.set(ret, customMappers.get(fld.getType()).unmarshall(valueFromDb));
                    continue;
                }
                if (fld.isAnnotationPresent(AdditionalData.class)) {
                    //this field should store all data that is not put to fields
                    if (!Map.class.isAssignableFrom(fld.getType())) {
                        log.error("Could not deserialize additional data into fld of type " + fld.getType().toString());
                        continue;
                    }
                    Set<String> keys = o.keySet();
                    Map<String, Object> data = new HashMap<>();
                    for (String k : keys) {
                        if (flds.contains(k)) {
                            continue;
                        }
                        if (k.equals("_id")) {
                            //id already mapped
                            continue;
                        }

                        if (o.get(k) instanceof Map) {
                            if (((Map<String, Object>) o.get(k)).get("class_name") != null) {
                                data.put(k, deserialize(annotationHelper.getClassForTypeId((String) ((Map<String, Object>) o.get(k)).get("class_name")), (Map<String, Object>) o.get(k)));
                            } else {
                                data.put(k, deserializeMap((Map<String, Object>) o.get(k)));
                            }
                        } else if (o.get(k) instanceof List && !((List) o.get(k)).isEmpty() && ((List) o.get(k)).get(0) instanceof Map) {
                            data.put(k, deserializeList((List<Map<String, Object>>) o.get(k)));
                        } else {
                            data.put(k, o.get(k));
                        }

                    }
                    fld.set(ret, data);
                    continue;
                }
                if (valueFromDb == null) {
                    if (!fld.getType().isPrimitive()) {
                        fld.set(ret, null);
                    }
                    continue;
                }
                if (fld.isAnnotationPresent(Encrypted.class)) {
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
                        valueFromDb = deserialize(fld.getType(), (String) valueFromDb);
                    } catch (Exception e) {
                        log.debug("Not a json string, cannot deserialize further");
                    }
                    annotationHelper.setValue(ret, valueFromDb, f);
                    continue;
                }
                Object value = null;
                if (!Collection.class.isAssignableFrom(fld.getType()) && fld.isAnnotationPresent(Reference.class)) {
                    //A reference - only id stored
                    Reference reference = fld.getAnnotation(Reference.class);
                    MorphiumReference r = null;
                    if (morphium == null) {
                        log.error("Morphium not set - could not de-reference!");
                    } else {
                        if (Map.class.isAssignableFrom(fld.getType())) {
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
                                Class type = fld.getType();
                                if (r != null) {
                                    if (r.getCollectionName() != null) {
                                        collectionName = r.getCollectionName();
                                    } else {
                                        collectionName = getCollectionName(annotationHelper.getClassForTypeId(r.getClassName()));
                                    }
                                    type = annotationHelper.getClassForTypeId(r.getClassName());
                                } else {
                                    if (annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Entity.class)) {
                                        collectionName = getCollectionName(fld.getType());
                                    }
                                }
                                if (collectionName == null) {
                                    throw new IllegalArgumentException("Could not create reference!");
                                }
                                if (reference.lazyLoading()) {
                                    List<String> lst = annotationHelper.getFields(fld.getType(), Id.class);
                                    if (lst.isEmpty()) {
                                        throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                                    }
                                    if (id instanceof String && annotationHelper.getField(fld.getType(), lst.get(0)).getType().equals(MorphiumId.class)) {
                                        id = new MorphiumId(id.toString());
                                    } else if (id instanceof ObjectId && annotationHelper.getField(fld.getType(), lst.get(0)).getType().equals(MorphiumId.class)) {
                                        id = new MorphiumId(((ObjectId) id).toByteArray());
                                    }
                                    value = morphium.createLazyLoadedEntity(fld.getType(), id, collectionName);
                                } else {
                                    try {
                                        value = morphium.findById(type, id, collectionName);
                                    } catch (MorphiumAccessVetoException ex) {
                                        log.info("not dereferencing due to veto from listener", ex);
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
                            String collection = getCollectionName(fld.getType());
                            if (r != null && r.getCollectionName() != null) {
                                collection = r.getCollectionName();
                            }
                            if (id != null) {
                                if (reference.lazyLoading()) {
                                    List<String> lst = annotationHelper.getFields(fld.getType(), Id.class);
                                    if (lst.isEmpty()) {
                                        throw new IllegalArgumentException("Referenced object does not have an ID? Is it an Entity?");
                                    }
                                    if (id instanceof String && annotationHelper.getField(fld.getType(), lst.get(0)).getType().equals(MorphiumId.class)) {
                                        id = new MorphiumId(id.toString());
                                    }
                                    value = morphium.createLazyLoadedEntity(fld.getType(), id, collection);
                                } else {
                                    //                                Query q = morphium.createQueryFor(fld.getSearchType());
                                    //                                q.f("_id").eq(id);
                                    try {
                                        value = morphium.findById(fld.getType(), id, collection);
                                    } catch (MorphiumAccessVetoException e) {
                                        log.info("not dereferencing due to veto from listener", e);
                                    }
                                }
                            } else {
                                value = null;
                            }
                        }
                    }
                } else if (fld.isAnnotationPresent(Id.class)) {
                    value = o.get("_id");
                    if (value != null && !value.getClass().equals(fld.getType())) {
                        log.debug("read value and field type differ...");
                        if (fld.getType().equals(MorphiumId.class)) {
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
                            if (fld.getType().equals(String.class)) {
                                value = value.toString();
                            } else if (fld.getType().equals(Long.class) || fld.getType().equals(long.class)) {
                                value = ((MorphiumId) value).getTime();
                            } else {
                                log.error("cannot convert - ID IS SET TO NULL. Type read from db is " + value.getClass().getName() + " - expected value is " + fld.getType().getName());
                                return null;
                            }
                        }
                    }
                } else if (annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Entity.class) || annotationHelper.isAnnotationPresentInHierarchy(fld.getType(), Embedded.class)) {
                    //entity! embedded
                    value = deserialize(fld.getType(), (HashMap<String, Object>) valueFromDb);
                    //                    List lst = new ArrayList<Object>();
                    //                    lst.add(value);
                    //                    morphium.firePostLoad(lst);

                } else if (Map.class.isAssignableFrom(fld.getType())) {
                    Map<String, Object> map = (Map<String, Object>) valueFromDb;
                    Map toFill = new HashMap();
                    if (map != null) {
                        fillMap((ParameterizedType) fld.getGenericType(), map, toFill, ret);
                    }
                    value = toFill;
                } else if (Collection.class.isAssignableFrom(fld.getType()) || fld.getType().isArray()) {
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
                            Collections.addAll(lst, (Object[]) valueFromDb);
                        }
                    } else {
                        List<Map<String, Object>> l = (List<Map<String, Object>>) valueFromDb;
                        if (l != null) {
                            // type is List<?> or ?[]
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
                            fillList(fld, fld.getAnnotation(Reference.class), type, l, lst, ret);
                        }
                    }
                    if (fld.getType().isArray()) {
                        Object arr = Array.newInstance(fld.getType().getComponentType(), lst.size());
                        for (int i = 0; i < lst.size(); i++) {
                            if (fld.getType().getComponentType().isPrimitive()) {
                                if (fld.getType().getComponentType().equals(int.class)) {
                                    if (lst.get(i) instanceof Double) {
                                        Array.set(arr, i, ((Double) lst.get(i)).intValue());
                                    } else if (lst.get(i) instanceof Integer) {
                                        Array.set(arr, i, (Integer) lst.get(i));
                                    } else if (lst.get(i) instanceof Long) {
                                        Array.set(arr, i, ((Long) lst.get(i)).intValue());
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }

                                } else if (fld.getType().getComponentType().equals(long.class)) {
                                    if (lst.get(i) instanceof Double) {
                                        Array.set(arr, i, ((Double) lst.get(i)).longValue());
                                    } else if (lst.get(i) instanceof Integer) {
                                        Array.set(arr, i, ((Integer) lst.get(i)).longValue());
                                    } else if (lst.get(i) instanceof Long) {
                                        Array.set(arr, i, (Long) lst.get(i));
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }

                                } else if (fld.getType().getComponentType().equals(float.class)) {
                                    //Driver sends doubles instead of floats
                                    if (lst.get(i) instanceof Double) {
                                        Array.set(arr, i, ((Double) lst.get(i)).floatValue());
                                    } else if (lst.get(i) instanceof Integer) {
                                        Array.set(arr, i, ((Integer) lst.get(i)).floatValue());
                                    } else if (lst.get(i) instanceof Long) {
                                        Array.set(arr, i, ((Long) lst.get(i)).floatValue());
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }

                                } else if (fld.getType().getComponentType().equals(double.class)) {
                                    if (lst.get(i) instanceof Float) {
                                        Array.set(arr, i, ((Float) lst.get(i)).doubleValue());
                                    } else if (lst.get(i) instanceof Integer) {
                                        Array.set(arr, i, ((Integer) lst.get(i)).doubleValue());
                                    } else if (lst.get(i) instanceof Long) {
                                        Array.set(arr, i, ((Long) lst.get(i)).doubleValue());
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }

                                } else if (fld.getType().getComponentType().equals(byte.class)) {
                                    if (lst.get(i) instanceof Integer) {
                                        Array.set(arr, i, ((Integer) lst.get(i)).byteValue());
                                    } else if (lst.get(i) instanceof Long) {
                                        Array.set(arr, i, ((Long) lst.get(i)).byteValue());
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }
                                } else if (fld.getType().getComponentType().equals(boolean.class)) {
                                    if (lst.get(i) instanceof String) {
                                        Array.set(arr, i, lst.get(i).toString().equalsIgnoreCase("true"));
                                    } else if (lst.get(i) instanceof Integer) {
                                        Array.set(arr, i, (Integer) lst.get(i) == 1);
                                    } else {
                                        Array.set(arr, i, lst.get(i));
                                    }

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
                    if (fld.getType().isEnum()) {
                        value = Enum.valueOf((Class<? extends Enum>) fld.getType(), (String) valueFromDb);
                    } else if (valueFromDb instanceof ObjectId) {
                        if (fld.getType().equals(MorphiumId.class)) {
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
                    } else {
                        value = valueFromDb;
                    }
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
                    } else if (field.getType().equals(String.class) && o.get("_id").getClass().equals(MorphiumId.class)) {
                        log.warn("ID type missmatch - field is string but got objectId from mongo - converting");
                        field.set(ret, o.get("_id").toString());
                    } else if (field.getType().equals(MorphiumId.class) && o.get("_id").getClass().equals(ObjectId.class)) {
                        field.set(ret, new MorphiumId(((ObjectId) o.get("_id")).toByteArray()));
                    } else if (field.getType().equals(ObjectId.class) && o.get("_id").getClass().equals(MorphiumId.class)) {
                        field.set(ret, new ObjectId(((MorphiumId) o.get("_id")).getBytes()));
                    } else if (field.getType().equals(ObjectId.class) && o.get("_id").getClass().equals(String.class)) {
                        field.set(ret, new ObjectId(((ObjectId) o.get("_id")).toString()));
                    } else if (field.getType().equals(MorphiumId.class) && o.get("_id").getClass().equals(String.class)) {
                        //                        log.warn("ID type missmatch - field is objectId but got string from db - trying conversion");
                        field.set(ret, new MorphiumId((String) o.get("_id")));
                    } else {
                        log.error("ID type missmatch");
                        throw new IllegalArgumentException("ID type missmatch. Field in '" + ret.getClass().toString() + "' is '" + field.getType().toString() + "' but we got '" + o.get("_id").getClass().toString() + "' from Mongo!");
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

    public Map deserializeMap(Map<String, Object> dbObject) {
        Map retMap = new HashMap(dbObject);
        if (dbObject != null) {
            for (String n : dbObject.keySet()) {
                retMap.put(n, unmarshallInternal(dbObject.get(n)));
            }
        } else {
            retMap = null;
        }
        return retMap;
    }

    private Object unmarshallInternal(Object val) {
        if (val instanceof Map) {
            Map<String, Object> mapVal = (Map<String, Object>) val;
            if (mapVal.containsKey("class_name") || mapVal.containsKey("className")) {
                //Entity to map!
                String cn = (String) mapVal.get("class_name");
                if (cn == null) {
                    cn = (String) mapVal.get("className");
                }
                try {
                    Class ecls = annotationHelper.getClassForTypeId(cn);
                    Object obj = deserialize(ecls, mapVal);
                    if (obj != null) {
                        return obj;
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else if (mapVal.containsKey("_b64data") || mapVal.containsKey("b64Data")) {
                String d = (String) mapVal.get("_b64data");
                if (d == null) {
                    d = (String) mapVal.get("b64Data");
                }
                Decoder dec = Base64.getMimeDecoder();
                ObjectInputStream in;
                try {
                    in = new ObjectInputStream(new ByteArrayInputStream(dec.decode(d)));
                    return in.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else {
                //maybe a normal map --> recurse
                return deserializeMap(mapVal);
            }
        } else if (val instanceof ObjectId) {
            val = new MorphiumId(((ObjectId) val).toByteArray());
        } else if (val instanceof List) {
            List<Map<String, Object>> lst = (List<Map<String, Object>>) val;
            return deserializeList(lst);
        }
        return val;
    }

    public List deserializeList(List<Map<String, Object>> lst) {
        return lst.stream().map(this::unmarshallInternal).collect(Collectors.toList());
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private void fillList(Field forField, Reference ref, ParameterizedType listType, List<Map<String, Object>> fromDB, List toFillIn, Object containerEntity) {
        fromDB = new ArrayList<>(fromDB); //avoiding concurrent changes!
        if (ref != null) {
            for (Map<String, Object> obj : fromDB) {
                if (obj == null) {
                    toFillIn.add(null);
                    continue;
                }

                MorphiumReference r = deserialize(MorphiumReference.class, obj);
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
        for (Object val : fromDB) {
            if (val instanceof Map) {
                boolean cont = false;

                if (cont) continue;
                //Override type if className is specified - needed for polymoprh lists etc.
                if (((Map<String, Object>) val).containsKey("class_name") || ((Map<String, Object>) val).containsKey("className")) {
                    //Entity to map!
                    String cn = (String) ((Map<String, Object>) val).get("class_name");
                    if (cn == null) {
                        cn = (String) ((Map<String, Object>) val).get("className");
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
                    Class cls = getElementClass(listType);
                    if (Map.class.isAssignableFrom(cls)) {
                        // that is an actual map!
                        HashMap mp = new HashMap();
                        fillMap((ParameterizedType) listType.getActualTypeArguments()[0], (Map<String, Object>) val, mp, containerEntity);
                        toFillIn.add(mp);
                        continue;
                    } else {
                        Entity entity = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
                        Embedded embedded = annotationHelper.getAnnotationFromHierarchy(cls, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);
                        if (entity != null || embedded != null) {
                            toFillIn.add(deserialize(cls, (Map<String, Object>) val));
                            continue;
                        }
                    }
                } else {
                    HashMap mp = new HashMap();
                    if (listType != null) {
                        fillMap((ParameterizedType) listType.getActualTypeArguments()[0], (Map<String, Object>) val, mp, containerEntity);
                        toFillIn.add(mp);
                    } else {
                        log.warn("Cannot de-reference to unknown collection type - trying object instead");
                        toFillIn.add(val);
                    }
                    continue;
                }
            } else if (val instanceof List) {
                //list in list
                if (listType != null) {
                    ArrayList lt = new ArrayList();
                    Class lstt = null;
                    try {
                        lstt = annotationHelper.getClassForTypeId(listType.getActualTypeArguments()[0].getTypeName());
                    } catch (ClassNotFoundException e) {
                        //could not find it, assuming list type
                    }
                    if (lstt == null || lstt.isAssignableFrom(List.class)) {
                        fillList(forField, ref, (ParameterizedType) listType.getActualTypeArguments()[0], (List<Map<String, Object>>) val, lt, containerEntity);
                        toFillIn.add(lt);
                    } else {
                        fillList(forField, ref, listType, (List<Map<String, Object>>) val, toFillIn, containerEntity);
                    }
                } else {
                    log.warn("Cannot de-reference to unknown collection - trying to add Object only");
                    toFillIn.add(val);
                }
                continue;

            }
            toFillIn.add(unmarshallInternal(val));
        }
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

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private void fillMap(ParameterizedType mapType, Map<String, Object> fromDB, Map toFillIn, Object containerEntity) {
        for (Entry<String, Object> entry : fromDB.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map) {
                if (mapType != null) {
                    //have a list of something
                    Class cls = getElementClass(mapType);
                    if (Map.class.isAssignableFrom(cls)) {
                        // this is an actual map
                        HashMap mp = new HashMap();
                        fillMap((ParameterizedType) mapType.getActualTypeArguments()[1], (Map<String, Object>) val, mp, containerEntity);
                        toFillIn.put(key, mp);
                        continue;
                    } else {
                        Entity entity = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
                        Embedded embedded = annotationHelper.getAnnotationFromHierarchy(cls, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);
                        if (entity != null || embedded != null) {
                            toFillIn.put(key, deserialize(cls, (Map<String, Object>) val));
                            continue;
                        }
                    }
                } else {
                    HashMap mp = new HashMap();
                    fillMap((ParameterizedType) mapType.getActualTypeArguments()[1], (Map<String, Object>) val, mp, containerEntity);
                    toFillIn.put(key, mp);
                    continue;
                }

            } else if (val instanceof List) {
                //list in list
                ArrayList lt = new ArrayList();
                fillList(null, null, (ParameterizedType) mapType.getActualTypeArguments()[1], (List<Map<String, Object>>) val, lt, containerEntity);
                toFillIn.put(key, lt);
                continue;

            }
            toFillIn.put(key, unmarshallInternal(val));
        }
    }
}
