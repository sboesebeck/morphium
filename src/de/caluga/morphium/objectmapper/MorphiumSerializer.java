package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.mapping.MorphiumTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

public class MorphiumSerializer {

    private final List<Class> mongoTypes;
    private final AnnotationAndReflectionHelper anhelper;
    private final Map<Class<?>, NameProvider> nameProviderByClass;
    private final Morphium morphium;
    private final MorphiumObjectMapper objectMapper;
    private final Logger log = LoggerFactory.getLogger(MorphiumSerializer.class);
    private final com.fasterxml.jackson.databind.ObjectMapper jackson;
    private final SimpleModule module;
    private final Map<Class, MorphiumTypeMapper> typeMapper;

    public MorphiumSerializer(AnnotationAndReflectionHelper ar, Map<Class<?>, NameProvider> np, Morphium m, MorphiumObjectMapper om, Map<Class, MorphiumTypeMapper> typeMapper) {
        mongoTypes = Collections.synchronizedList(new ArrayList<>());
        anhelper = ar;
        nameProviderByClass = np;
        morphium = m;
        this.typeMapper = typeMapper;

        objectMapper = om;
        module = new SimpleModule();

        mongoTypes.add(String.class);
        mongoTypes.add(Character.class);
        mongoTypes.add(Integer.class);
        mongoTypes.add(Long.class);
        mongoTypes.add(Float.class);
        mongoTypes.add(Double.class);
        mongoTypes.add(Date.class);
        mongoTypes.add(Boolean.class);
        mongoTypes.add(Byte.class);


        module.addSerializer(MorphiumId.class, new JsonSerializer<MorphiumId>() {
            @Override
            public void serialize(MorphiumId morphiumId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
//                jsonGenerator.writeObjectId(morphiumId.toString());
                jsonGenerator.writeStartObject();
                jsonGenerator.writeFieldName("morphium id");
                jsonGenerator.writeObject(morphiumId.toString());
                jsonGenerator.writeEndObject();
//                jsonGenerator.writeObject(morphiumId.toString());
            }
        });

        module.addSerializer(Collection.class, new JsonSerializer<Collection>() {
            @Override
            public void serialize(Collection list, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeStartArray();
                for (Object o : list) {
                    Map m = null;
                    if (o == null) {
                        jsonGenerator.writeNull();
                        continue;
                    }
                    if (o instanceof Enum) {
                        m = new HashMap();
                        m.put("name", ((Enum) o).name());
                    } else if (mongoTypes.contains(o.getClass())) {
                        jsonGenerator.writeObject(o);
                        continue;
                    }
                    if (o instanceof Collection) {
                        serialize((Collection) o, jsonGenerator, serializerProvider);
                        continue;
//                        } else if (o instanceof MorphiumId) {
//                            jsonGenerator.write(o.toString());
//                            continue;
//                            jsonGenerator.writeString("ObjectId(" + o.toString() + ")");
//                            continue;
                    } else if (o instanceof Map) {
                        //m = new LinkedHashMap();
                        jsonGenerator.writeStartObject();
                        for (Map.Entry e : (Set<Map.Entry>) ((Map) o).entrySet()) {
                            if (mongoTypes.contains(e.getValue().getClass())) {
                                jsonGenerator.writeObjectField((String) e.getKey(), e.getValue());
                            } else if (e.getValue() instanceof Collection) {
                                jsonGenerator.writeFieldName((String) e.getKey());
                                serialize((Collection) e.getValue(), jsonGenerator, serializerProvider);
                            } else {
                                Map value = jackson.convertValue(e.getValue(), Map.class);
                                value.put("class_name", e.getValue().getClass().getName());
                                jsonGenerator.writeObjectField((String) e.getKey(), value);
                            }
                        }
                        //jsonGenerator.writeObject(m);
                        jsonGenerator.writeEndObject();
                        continue;
                    } else {
                        m = jackson.convertValue(o, Map.class);
                    }


                    m.put("class_name", o.getClass().getName());
                    jsonGenerator.writeObject(m);
                }
                jsonGenerator.writeEndArray();
            }
        });


//        ScanResult res = new FastClasspathScanner("").scan();
//
//        for (String n:res.getNamesOfClassesWithAnnotation(Entity.class)){
//            log.info("Found Entity: "+n);
//            Class<?> cls = res.classNameToClassRef(n);
//
//        }
        module.setSerializerModifier(new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
//                if (Map.class.isAssignableFrom(beanDesc.getBeanClass())){
//                    return new CustomMapSerializer((JsonSerializer<Map>)serializer,anhelper);
//                }
                if (anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Entity.class) || anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Embedded.class)) {
                    return new EntitySerializer((JsonSerializer<Object>) serializer, anhelper);
                }
                if (beanDesc.getBeanClass().isEnum()) {
                    return new CustomEnumSerializer();
                }
                if (typeMapper.containsKey(beanDesc.getBeanClass())) {
                    return new JsonSerializer<Object>() {
                        @Override
                        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                            gen.writeObject(typeMapper.get(value.getClass()).marshall(value));
                        }
                    };
                }
                if (Map.class.isAssignableFrom(beanDesc.getBeanClass())) {
                    return new JsonSerializer<Map>() {
                        @Override
                        public void serialize(Map value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                            Map ret = new LinkedHashMap();
                            for (Map.Entry e : (Set<Map.Entry>) value.entrySet()) {
                                if (anhelper.isEntity(e.getValue())) {
                                    Map value1 = jackson.convertValue(e.getValue(), Map.class);
                                    value1.put("class_name", e.getValue().getClass().getName());
                                    ret.put(e.getKey(), value1);
                                } else if (e.getValue() instanceof Map) {
                                    //map in a map
                                    gen.writeFieldName((String) e.getKey());
                                    serialize((Map) e.getValue(), gen, serializers);

                                } else {
                                    ret.put(e.getKey(), jackson.convertValue(e.getValue(), Map.class));
                                }
                            }
                            gen.writeObject(ret);
                        }

                    };
                }
                return serializer;
            }
        });
        jackson = new com.fasterxml.jackson.databind.ObjectMapper();
        jackson.registerModule(module);
        jackson.setVisibility(jackson.getSerializationConfig()
                .getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.ANY));
    }


    public Map<String, Object> serialize(Object o) {
//        try {
//            jackson.writeValue(System.out, o);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Map m = jackson.convertValue(o, Map.class);
        m = (Map) Utils.replaceMorphiumIds(m);
        return m;
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


    public class CustomEnumSerializer extends JsonSerializer<Enum> {

        @Override
        public void serialize(Enum anEnum, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            Map obj = new HashMap();
            obj.put("name", anEnum.name());
            obj.put("class_name", anEnum.getClass().getName());
            jsonGenerator.writeObject(obj);
        }
    }

//
//    public class CustomMapSerializer extends JsonSerializer<Map> {
//        private final AnnotationAndReflectionHelper an;
//        private JsonSerializer<Map> def;
//
//        public CustomMapSerializer(JsonSerializer<Map> def, AnnotationAndReflectionHelper an) {
//            this.def = def;
//            this.an = an;
//        }
//
//        @Override
//        public void serialize(Map map, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
//            for (Map.Entry entry:(Set<Map.Entry>)map.entrySet()){
//                if (entry.getValue().getClass().isEnum()){
//                    Map ser=new HashMap();
//                    ser.put("class_name",entry.getValue().getClass().getName());
//                    ser.put("name",((Enum)entry.getValue()).name());
//                    map.put(entry.getKey(),ser);
//                }
//            }
//            def.serialize(map,jsonGenerator,serializerProvider);
//        }
//    }

    public class EntitySerializer extends JsonSerializer<Object> {
        private final AnnotationAndReflectionHelper an;
        private JsonSerializer<Object> def;

        public EntitySerializer(JsonSerializer<Object> def, AnnotationAndReflectionHelper an) {
            this.def = def;
            this.an = an;
        }

        @Override
        public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            if (o == null) {
                jsonGenerator.writeNull();
                jsonGenerator.writeEndObject();
                return;
            }

            Entity entity = anhelper.getAnnotationFromHierarchy(o.getClass(), Entity.class);
            Embedded embedded = anhelper.getAnnotationFromHierarchy(o.getClass(), Embedded.class);
            for (Field fld : an.getAllFields(o.getClass())) {
                try {
                    fld.setAccessible(true);
                    Object value = fld.get(o);
                    Transient tr = fld.getAnnotation(Transient.class);
                    if (tr != null) continue;
                    ReadOnly ro = fld.getAnnotation(ReadOnly.class);
                    if (ro != null) continue; //not serializing it => not storing it
                    Reference r = fld.getAnnotation(Reference.class);
                    if (r != null && value != null) {
                        //create reference
                        if (value instanceof List) {
                            List ret = new ArrayList();
                            //list of references!
                            for (Object lel : (List) value) {
                                if (lel == null) {
                                    ret.add(null);
                                    continue;
                                }
                                Object id = anhelper.getId(lel);
                                if (id == null && r.automaticStore()) {
                                    morphium.storeNoCache(lel);
                                    id = anhelper.getId(lel);
                                }
                                MorphiumReference ref = new MorphiumReference(lel.getClass().getName(), id);
                                ref.setLazy(r.lazyLoading());
                                ret.add(ref);
                            }
                            value = ret;
                        } else {
                            Object id = anhelper.getId(value);
                            if (id == null && r.automaticStore()) {
                                morphium.storeNoCache(value);
                                id = anhelper.getId(value);
                            }
                            MorphiumReference ref = new MorphiumReference(value.getClass().getName(), id);
                            ref.setLazy(r.lazyLoading());
                            ref.setCollectionName(getCollectionName(value.getClass()));
                            value = ref;
                        }
                    }

                    if (value instanceof Map) {
                        Map ret = new LinkedHashMap();

                        for (Map.Entry e : ((Set<Map.Entry>) ((Map) value).entrySet())) {
                            if (e.getValue() instanceof Map) {

                                ret.put(e.getKey(), serializeMap((Map) e.getValue()));
                            } else if (anhelper.isEntity(e.getValue())) {
                                ret.put(e.getKey(), jackson.convertValue(e.getValue(), Map.class));
                                ((Map) ret.get(e.getKey())).put("class_name", e.getValue().getClass().getName());
                                ((Map) ret.get(e.getKey())).remove("_id");

                            } else {

                                ret.put(e.getKey(), e.getValue());
                            }
                        }
                        value = ret;
                    }


                    UseIfnull un = fld.getAnnotation(UseIfnull.class);
                    if (value != null || un != null) {
                        String fldName = an.getFieldName(o.getClass(), fld.getName());
                        if (anhelper.isEntity(value)) {
                            Map ret = jackson.convertValue(value, Map.class);
                            ret.remove("_id");
                            value = ret;
                        }
                        jsonGenerator.writeObjectField(fldName, value);
                        continue;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (entity != null && entity.polymorph() || embedded != null && embedded.polymorph()) {
                jsonGenerator.writeObjectField("class_name", o.getClass().getName());
            }
            jsonGenerator.writeEndObject();
        }

    }

    private Map serializeMap(Map value) {
        Map ret = new LinkedHashMap();
        for (Map.Entry e : (Set<Map.Entry>) value.entrySet()) {
            if (anhelper.isEntity(e.getValue())) {
                Map value1 = jackson.convertValue(e.getValue(), Map.class);
                value1.put("class_name", e.getValue().getClass().getName());
                ret.put(e.getKey(), value1);
            } else if (e.getValue() instanceof Map) {
                //map in a map

                ret.put(e.getKey(), serializeMap((Map) e.getValue()));

            } else {
                if (mongoTypes.contains(e.getValue().getClass())) {
                    ret.put(e.getKey(), e.getValue());
                } else {
                    ret.put(e.getKey(), jackson.convertValue(e.getValue(), Map.class));
                }
            }
        }
        return ret;
    }
}
