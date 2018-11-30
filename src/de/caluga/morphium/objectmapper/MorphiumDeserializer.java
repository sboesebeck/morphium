package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.introspect.POJOPropertyBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.mapping.MorphiumTypeMapper;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.reflect.ReflectionFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.*;
import java.util.*;

public class MorphiumDeserializer {

    private final AnnotationAndReflectionHelper anhelper;
    private final Morphium morphium;

    private final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();
    private final Logger log = LoggerFactory.getLogger(MorphiumSerializer.class);
    private final com.fasterxml.jackson.databind.ObjectMapper jackson;

    public MorphiumDeserializer(AnnotationAndReflectionHelper anhelper, Map<Class<?>, NameProvider> nameProviderByClass, Morphium morphium, Map<Class, MorphiumTypeMapper> typeMapper) {

        this.anhelper = anhelper;
        this.morphium = morphium;
        SimpleModule module = new SimpleModule();
        jackson = new com.fasterxml.jackson.databind.ObjectMapper();
        jackson.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jackson.setVisibility(jackson.getSerializationConfig()
                .getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.ANY));

//        module.addDeserializer(MorphiumId.class, new JsonDeserializer<MorphiumId>() {
//            @Override
//            public MorphiumId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
//                try {
//                    String n = jsonParser.nextFieldName();
//                    String valueAsString = jsonParser.getValueAsString();
//                    jsonParser.nextToken();
//                    if (valueAsString == null) return null;
//                    return new MorphiumId(valueAsString);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//        });
        module.setDeserializerModifier(new BeanDeserializerModifier() {
            @SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
            @Override
            public List<BeanPropertyDefinition> updateProperties(DeserializationConfig config, BeanDescription beanDesc, List<BeanPropertyDefinition> propDefs) {

                if (anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Entity.class) || anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Embedded.class)) {
                    List<BeanPropertyDefinition> lst = new ArrayList<>();
                    for (BeanPropertyDefinition d : propDefs) {
                        Field fld = anhelper.getField(beanDesc.getBeanClass(), d.getName());
                        if (fld == null) {
                            return lst;
                        }
                        PropertyName pn = new PropertyName(anhelper.getFieldName(beanDesc.getBeanClass(), fld.getName()));
                        BeanPropertyDefinition def = new POJOPropertyBuilder(config, null, false, pn);
                        ((POJOPropertyBuilder) def).addField(d.getField(), pn, false, true, false);
                        lst.add(def);
                    }
                    return lst;
                } else {
                    return propDefs;
                }

            }

            @Override
            public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {
                JsonDeserializer<?> def = super.modifyDeserializer(config, beanDesc, deserializer);
                if (anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Entity.class) || anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Embedded.class)) {

                    return new EntityDeserializer(beanDesc.getBeanClass(), anhelper);
                }
                if (typeMapper.containsKey(beanDesc.getBeanClass())) {
                    return new JsonDeserializer<Object>() {
                        @Override
                        public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
                            return typeMapper.get(beanDesc.getBeanClass()).unmarshall(jsonParser.readValueAs(Object.class));
                        }
                    };
                }

                if (Collection.class.isAssignableFrom(beanDesc.getBeanClass())) {
                    return new JsonDeserializer<Collection>() {
                        @Override
                        public Collection deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
                            Collection toAdd;
                            try {
                                toAdd = (Collection) beanDesc.getBeanClass().newInstance();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            List in = jsonParser.readValueAs(List.class);
                            for (Object e : in) {
                                if (e instanceof Map) {
                                    if (((Map) e).get("class_name") != null) {
                                        try {
                                            Class<?> clsName = anhelper.getClassForTypeId((String) ((Map) e).get("class_name"));

                                            //noinspection unchecked
                                            toAdd.add(jackson.convertValue(e, clsName));
                                        } catch (ClassNotFoundException e1) {
                                            //TODO: Implement Handling
                                            throw new RuntimeException(e1);
                                        }
                                        continue;
                                    } else if (((Map) e).get("original_class_name") != null) {
                                        try {
                                            Object toPut = jackson.convertValue(e, BinarySerializedObject.class);
                                            BASE64Decoder dec = new BASE64Decoder();
                                            try {
                                                ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(dec.decodeBuffer(((BinarySerializedObject) toPut).getB64Data())));
                                                toPut = oin.readObject();
                                                //noinspection unchecked
                                                toAdd.add(toPut);
                                            } catch (IOException e1) {
                                                log.error("Could not deserialize", e);
                                            }
                                        } catch (ClassNotFoundException e1) {
                                            e1.printStackTrace();
                                        }
                                        continue;
                                    }
                                }
                                //noinspection unchecked
                                toAdd.add(e);
                            }


                            return toAdd;
                        }
                    };
                }
                if (beanDesc.getBeanClass().isEnum()) {
                    return new JsonDeserializer<Enum>() {
                        @Override
                        public Enum deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
                            @SuppressWarnings("unchecked") Map<String, String> m = (Map<String, String>) jsonParser.readValueAs(Map.class);
                            Class<Enum> target;
                            try {
                                //noinspection unchecked
                                target = (Class<Enum>) anhelper.getClassForTypeId(m.get("class_name"));
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }

                            //noinspection unchecked
                            return Enum.valueOf(target, m.get("name"));
                        }
                    };
                }
                return def;
            }
        });
        jackson.registerModule(module);
    }

    public <T> T deserialize(Class<? extends T> theClass, Map<String, Object> o) {
        if (o.containsKey("in_answer_to")) {
            log.info("got answer");
        }
        Object ret = replaceMorphiumIds(o);
        return jackson.convertValue(ret, theClass);
    }

    private Object replaceMorphiumIds(Object o) {
        if (o instanceof Map) {
            //noinspection unchecked
            return replaceMorphiumIds((Map) o);
        } else if (o instanceof ObjectId) {
            return new MorphiumId(((ObjectId) o).toHexString());
        }
        return o;
    }

    private Object replaceMorphiumIds(Map<String, Object> m) {
        Map ret = new HashMap();
        for (Map.Entry e : m.entrySet()) {
            if (e.getValue() instanceof Map) {
                //noinspection unchecked,unchecked
                ret.put(e.getKey(), replaceMorphiumIds((Map<String, Object>) e.getValue()));
            } else if (e.getValue() instanceof MorphiumId) {
                Map mid = new HashMap();
                //noinspection unchecked
                mid.put("morphium id", e.getValue().toString());
                //noinspection unchecked
                ret.put(e.getKey(), mid);
            } else if (e.getValue() instanceof ObjectId) {
                //noinspection unchecked
                ret.put(e.getKey(), new MorphiumId(((ObjectId) e.getValue()).toByteArray()));
            } else if (e.getValue() instanceof List) {
                //noinspection unchecked
                ret.put(e.getKey(), replaceMorphiumIds((List) e.getValue()));
            } else {
                //noinspection unchecked
                ret.put(e.getKey(), e.getValue());
            }
        }
        return ret;
    }

    private Object replaceMorphiumIds(List value) {
        List ret = new ArrayList();
        for (Object o : value) {
            if (o instanceof MorphiumId) {
                Map m = new HashMap();
                //noinspection unchecked
                m.put("morphium id", o.toString());
                //noinspection unchecked
                ret.add(m);
            } else if (o instanceof Map) {
                //noinspection unchecked,unchecked
                ret.add(replaceMorphiumIds((Map) o));
            } else if (o instanceof List) {
                //noinspection unchecked
                ret.add(replaceMorphiumIds((List) o));
            } else {
                //noinspection unchecked
                ret.add(o);
            }
        }
        return ret;
    }

    private Map handleMap(Type type, Map m) throws ClassNotFoundException {
        Map v = new LinkedHashMap();
        Class valueType = null;
        if (type != null) {
            if (type instanceof ParameterizedType) {
                if (((ParameterizedType) type).getActualTypeArguments()[1] instanceof ParameterizedType) {
                    //map of Maps
                    //ParameterizedType t2= (ParameterizedType) ((ParameterizedType)type).getActualTypeArguments()[0];
                    type = ((ParameterizedType) type).getActualTypeArguments()[1];
                } else {
                    valueType = (Class) ((ParameterizedType) type).getActualTypeArguments()[1];
                }
            }
        }
        //noinspection unchecked
        for (Map.Entry e : (Set<Map.Entry>) m.entrySet()) {
            if (e.getValue() instanceof Map) {
                //Object? Enum? in a Map... Map<Something,MAP>
                Object toPut;

                Map ev = (Map) e.getValue();
                Class cls;
                if (ev.get("class_name") != null) {
                    cls = anhelper.getClassForTypeId((String) ev.get("class_name"));
                    if (cls.isEnum()) {
                        //noinspection unchecked
                        toPut = Enum.valueOf(cls, (String) ev.get("name"));
                    } else {
                        //noinspection unchecked
                        toPut = jackson.convertValue(ev, cls);
                    }
                } else if (ev.get("original_class_name") != null) {
                    toPut = jackson.convertValue(ev, BinarySerializedObject.class);
                    BASE64Decoder dec = new BASE64Decoder();
                    try {
                        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(dec.decodeBuffer(((BinarySerializedObject) toPut).getB64Data())));
                        toPut = in.readObject();
                    } catch (IOException e1) {
                        log.error("Could not deserialize", e);
                    }
                } else if (ev.get("morphium id") != null) {
                    toPut = new MorphiumId((String) ev.get("morphium id"));
                } else {
                    toPut = handleMap(type, ev);
                }
                //noinspection unchecked
                v.put(e.getKey(), toPut);

            } else if (e.getValue() instanceof Collection) {
                ///Map<Something,List>
                Object retLst;
                retLst = handleList(type, (Collection) e.getValue());
                //noinspection unchecked
                v.put(e.getKey(), retLst);
            } else {
                //noinspection unchecked
                v.put(e.getKey(), e.getValue());
            }
        }
        return v;
    }

    @SuppressWarnings({"unchecked", "ConditionCoveredByFurtherCondition"})
    private Object handleList(Type type, Collection listIn) throws ClassNotFoundException {
        Class listElementType = null;
        Collection listOut;
        try {
            if (type instanceof Class) {
                if (((Class) type).isInterface()) {
                    if (List.class.isAssignableFrom((Class<?>) type)) {
                        listOut = new ArrayList();
                    } else if (Set.class.isAssignableFrom((Class<?>) type)) {
                        listOut = new LinkedHashSet();
                    } else {
                        listOut = new ArrayList();
                    }
                } else if (((Class) type).isArray()) {
                    listOut = new ArrayList();
                } else {
                    listOut = (Collection) ((Class) type).newInstance();
                }
            } else if (type instanceof ParameterizedType) {
                Class cls = ((Class) ((ParameterizedType) type).getRawType());
                if (cls.isInterface()) {
                    if (cls.equals(Set.class)) {
                        listOut = new LinkedHashSet();
                    } else if (cls.equals(List.class)) {
                        listOut = new ArrayList();
                    } else {
                        listOut = new ArrayList(); //todo: Check
                    }
                } else {
                    listOut = (Collection) cls.newInstance();
                }
            } else {
                listOut = new ArrayList();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (type != null) {
            if (type instanceof ParameterizedType) {
                if (((ParameterizedType) type).getActualTypeArguments()[0] instanceof ParameterizedType) {
                    //list of lists? list of maps?
                    //ParameterizedType t2= (ParameterizedType) ((ParameterizedType)type).getActualTypeArguments()[0];
                    type = ((ParameterizedType) type).getActualTypeArguments()[0];
                    listElementType = null;
                } else if (((ParameterizedType) type).getActualTypeArguments()[0] instanceof WildcardType) {
                    type = ((WildcardType) (((ParameterizedType) type).getActualTypeArguments()[0])).getUpperBounds()[0];
                    listElementType = null;
                } else {
                    listElementType = (Class) ((ParameterizedType) type).getActualTypeArguments()[0];
                }
            }
        }
        for (Object el : listIn) {
            if (el instanceof Map) {
                if (((Map) el).get("morphium id") != null) {
                    listOut.add(new MorphiumId((String) ((Map) el).get("morphium id")));
                } else if (((Map) el).get("referenced_class_name") != null && ((Map) el).get("refid") != null) {
                    //morphium reference - deReferencing
                    MorphiumReference ref = jackson.convertValue(el, MorphiumReference.class);
                    Object t;
                    if (ref.isLazy()) {
                        t = morphium.createLazyLoadedEntity(anhelper.getClassForTypeId(ref.getClassName()), ref.getId(), ref.getCollectionName());
                    } else {
                        t = morphium.findById(anhelper.getClassForTypeId(ref.getClassName()), ref.getId(), ref.getCollectionName());
                    }
                    listOut.add(t);
                } else if (((Map) el).get("class_name") != null) {
                    Class<?> cls = anhelper.getClassForTypeId((String) ((Map) el).get("class_name"));
                    if (cls.isEnum()) {
                        listOut.add(Enum.valueOf((Class) cls, (String) ((Map) el).get("name")));
                    } else {
                        listOut.add(jackson.convertValue(el, cls));
                    }
                } else if (((Map) el).get("original_class_name") != null) {
                    Object toPut = jackson.convertValue(el, BinarySerializedObject.class);
                    BASE64Decoder dec = new BASE64Decoder();
                    try {
                        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(dec.decodeBuffer(((BinarySerializedObject) toPut).getB64Data())));
                        toPut = in.readObject();
                    } catch (IOException e1) {
                        log.error("Could not deserialize", e1);
                    }
                    listOut.add(toPut);
                } else {
                    if (listElementType != null) {
                        listOut.add(jackson.convertValue(el, listElementType));
                    } else {
                        listOut.add(handleMap(null, (Map) el));
                    }
                }
            } else if (el instanceof List) {
                listOut.add(handleList(type, (List) el));
            } else if (listElementType != null) {
                listOut.add(jackson.convertValue(el, listElementType));
            } else {
                listOut.add(el);
            }

        }

        // convert to Array
        if (type != null && type instanceof Class && ((Class) type).isArray()) {
            Object arr = Array.newInstance(((Class) type).getComponentType(), listOut.size());
            int i = 0;
            for (Object listOutObj : listOut) {
                if (((Class) type).getComponentType().isPrimitive()) {
                    if (((Class) type).getComponentType().equals(int.class)) {
                        if (listOutObj instanceof Double) {
                            Array.set(arr, i, ((Double) listOutObj).intValue());
                        } else if (listOutObj instanceof Integer) {
                            Array.set(arr, i, listOutObj);
                        } else if (listOutObj instanceof Long) {
                            Array.set(arr, i, ((Long) listOutObj).intValue());
                        } else {
                            //noinspection RedundantCast
                            Array.set(arr, i, listOutObj);
                        }

                    } else if (((Class) type).getComponentType().equals(long.class)) {
                        if (listOutObj instanceof Double) {
                            Array.set(arr, i, ((Double) listOutObj).longValue());
                        } else if (listOutObj instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOutObj).longValue());
                        } else if (listOutObj instanceof Long) {
                            Array.set(arr, i, listOutObj);
                        } else {
                            Array.set(arr, i, listOutObj);
                        }

                    } else if (((Class) type).getComponentType().equals(float.class)) {
                        //Driver sends doubles instead of floats
                        if (listOutObj instanceof Double) {
                            Array.set(arr, i, ((Double) listOutObj).floatValue());
                        } else if (listOutObj instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOutObj).floatValue());
                        } else if (listOutObj instanceof Long) {
                            Array.set(arr, i, ((Long) listOutObj).floatValue());
                        } else {
                            Array.set(arr, i, listOutObj);
                        }

                    } else if (((Class) type).getComponentType().equals(double.class)) {
                        if (listOutObj instanceof Float) {
                            Array.set(arr, i, ((Float) listOutObj).doubleValue());
                        } else if (listOutObj instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOutObj).doubleValue());
                        } else if (listOutObj instanceof Long) {
                            Array.set(arr, i, ((Long) listOutObj).doubleValue());
                        } else {
                            Array.set(arr, i, listOutObj);
                        }

                    } else if (((Class) type).getComponentType().equals(byte.class)) {
                        if (listOutObj instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOutObj).byteValue());
                        } else if (listOutObj instanceof Long) {
                            Array.set(arr, i, ((Long) listOutObj).byteValue());
                        } else {
                            Array.set(arr, i, listOutObj);
                        }
                    } else if (((Class) type).getComponentType().equals(boolean.class)) {
                        if (listOutObj instanceof String) {
                            Array.set(arr, i, listOutObj.toString().equalsIgnoreCase("true"));
                        } else if (listOutObj instanceof Integer) {
                            Array.set(arr, i, (Integer) listOutObj == 1);
                        } else {
                            Array.set(arr, i, listOutObj);
                        }

                    }
                } else {
                    Array.set(arr, i, listOutObj);
                }
                i++;
            }
            return arr;
        }

        return listOut;
    }

    private class EntityDeserializer extends JsonDeserializer<Object> {
        private final AnnotationAndReflectionHelper anhelper;
        private final Logger log = LoggerFactory.getLogger(EntityDeserializer.class);
        private final Class type;

        public EntityDeserializer(Class cls, AnnotationAndReflectionHelper an) {
            type = cls;
            anhelper = an;
        }

        @Override
        public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
            try {
                Object ret = null;
                if (type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
                    Map m = jsonParser.readValueAs(Map.class);
                    if (m.get("class_name") != null) {
                        //noinspection unchecked
                        ret = jackson.convertValue(m, anhelper.getClassForTypeId(m.get("class_name").toString()));
                    }
                    return ret;
                }
                try {
                    ret = type.newInstance();
                } catch (Exception ignored) {
                }
                if (ret == null) {
                    final Constructor<Object> constructor;
                    try {
                        //noinspection unchecked
                        constructor = (Constructor<Object>) reflection.newConstructorForSerialization(type, Object.class.getDeclaredConstructor());
                        ret = constructor.newInstance();
                    } catch (Exception e) {
                        log.error("Exception during instanciation of type " + type.getName(), e);
                    }
                }
                String adField = anhelper.getAdditionalDataField(type);
                JsonToken tok;
                String currentName = "";
                while (true) {
                    tok = jsonParser.nextToken();
                    if (tok == null) return ret;
                    if (tok.equals(JsonToken.FIELD_NAME)) {
                        currentName = jsonParser.getCurrentName();
                        continue;
                    }
                    Field f = anhelper.getField(type, currentName);

                    if (tok.equals(JsonToken.START_ARRAY)) {
                        ////////////////////
                        ////// list or array
                        //////
                        List o = jackson.readValue(jsonParser, List.class);
                        if (f == null && adField != null) {
                            anhelper.getField(type, adField).set(ret, o);
                        } else if (f != null) {
                            f.set(ret, handleList(f.getGenericType(), o));
                        }
                        continue;

                    }
                    if (tok.equals(JsonToken.START_OBJECT)) {
                        ////////////////////
                        //object value
                        if (f != null && f.getType().isEnum()) {
                            Map v = jsonParser.readValueAs(Map.class);
                            //noinspection unchecked
                            f.set(ret, Enum.valueOf((Class) f.getType(), (String) v.get("name")));
                            continue;
                        }
                        if (f != null) {
                            //todo: list of references!
                            Reference r = f.getAnnotation(Reference.class);
                            if (r != null) {
                                if (Map.class.isAssignableFrom(f.getType())) {
                                    //map of references
                                    Map toSet = new LinkedHashMap();
                                    Map in = jackson.readValue(jsonParser, Map.class);
                                    //noinspection unchecked
                                    for (Map.Entry entry : (Set<Map.Entry>) in.entrySet()) {
                                        MorphiumReference ref = jackson.convertValue(entry.getValue(), MorphiumReference.class);
                                        Object id = replaceMorphiumIds(ref.getId());
                                        Class<?> cls = Class.forName(ref.getClassName());
                                        if (ref.isLazy()) {
                                            //noinspection unchecked
                                            toSet.put(entry.getKey(), morphium.createLazyLoadedEntity(cls, id, ref.getCollectionName()));
                                        } else {
                                            Object refObj = morphium.findById(cls, id);
                                            //noinspection unchecked
                                            toSet.put(entry.getKey(), refObj);
                                        }
                                    }
                                    f.set(ret, toSet);
                                } else {
                                    MorphiumReference ref = jackson.readValue(jsonParser, MorphiumReference.class);
                                    if (r.lazyLoading()) {
                                        f.set(ret, morphium.createLazyLoadedEntity(f.getType(), ref.getId(), ref.getCollectionName()));
                                    } else {

                                        Object id = replaceMorphiumIds(ref.getId());
                                        Object refObj = morphium.findById(f.getType(), id);
                                        f.set(ret, refObj);
                                    }
                                }
                                continue;
                            }
                            if (Map.class.isAssignableFrom(f.getType())) {
                                Map m = (Map) jackson.readValue(jsonParser, f.getType());
                                Map v = handleMap(f.getGenericType(), m);
                                f.set(ret, v);
                            } else {
                                if (f.getType().equals(String.class)) {
                                    f.set(ret, jackson.readValue(jsonParser, Object.class).toString());
                                } else if (f.getType().equals(MorphiumId.class)) {
                                    ObjectId v = jackson.readValue(jsonParser, ObjectId.class);
                                    f.set(ret, new MorphiumId(v.toHexString())/*new MorphiumId(v.get("morphium id").toString())*/);
                                } else {
                                    Map v = jackson.readValue(jsonParser, Map.class);
                                    if (v.containsKey("morphium id")) {
                                        f.set(ret, new MorphiumId(v.get("morphium id").toString()));
                                    } else {
                                        f.set(ret, jackson.convertValue(v, f.getType()));
                                    }
                                }
                            }
                        } else {
                            //just read the value and ignore it
                            readAdditionalValue(jsonParser, ret, adField, currentName);
                        }

                        continue;
                    }


                    if (tok.equals(JsonToken.END_OBJECT)) {
                        return ret;
                    }

                    if (f == null) {
                        readAdditionalValue(jsonParser, ret, adField, currentName);
                        continue;
                    }

                    ///reading in values!
//                    if (f.getType().isEnum()) {
//                        Map m = jsonParser.readValueAs(Map.class);
//                        f.set(ret, (Enum.valueOf((Class<Enum>) Class.forName((String) m.get("class_name")), (String) m.get("name"))));
//                        continue;
//                    }
//                    if (Map.class.isAssignableFrom(f.getType())) {
//                        Map m = jsonParser.readValueAs(Map.class);
//                        Map res = new HashMap();
//                        for (Map.Entry en : ((Map<Object, Object>) m).entrySet()) {
//                            if (en.getValue() instanceof Map) {
//                                String clsName = (String) ((Map) en.getValue()).get("class_name");
//                                if (clsName != null) {
//                                    Class<?> toValueType = Class.forName(clsName);
//                                    if (toValueType.isEnum()) {
//                                        res.put(en.getKey(), Enum.valueOf((Class<Enum>) toValueType, (String) ((Map) en.getValue()).get("name")));
//                                    } else {
//                                        res.put(en.getKey(), jackson.convertValue(en.getValue(), toValueType));
//                                    }
//                                    continue;
//                                }
//                            }
//                            res.put(en.getKey(), en.getValue());
//                        }to
//                        f.set(ret, res);
//                        continue;
//                    }
//


                    f.set(ret, jackson.readValue(jsonParser, f.getType()));

                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        private void readAdditionalValue(JsonParser jsonParser, Object ret, String adField, String currentName) throws IllegalAccessException, IOException {
            if (adField != null) {
                Field field = anhelper.getField(type, adField);
                if (field.get(ret) == null) {
                    field.set(ret, new LinkedHashMap<>());
                }
                //noinspection unchecked
                ((Map) field.get(ret)).put(currentName, jackson.readValue(jsonParser, Object.class));
            } else {
                //ignore value
                jackson.readValue(jsonParser, Object.class);
            }
        }
    }
}