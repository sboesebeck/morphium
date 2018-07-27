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
import de.caluga.morphium.mapping.BigIntegerTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.ReflectionFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.*;

public class MorphiumDeserializer {

    private final AnnotationAndReflectionHelper anhelper;
    private final Map<Class<?>, NameProvider> nameProviderByClass;
    private final Morphium morphium;

    private final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();
    private final Logger log = LoggerFactory.getLogger(MorphiumSerializer.class);
    private final SimpleModule module;
    private final com.fasterxml.jackson.databind.ObjectMapper jackson;
    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;

    public MorphiumDeserializer(AnnotationAndReflectionHelper anhelper, Map<Class<?>, NameProvider> nameProviderByClass, Morphium morphium, MorphiumObjectMapper objectMapper) {

        this.anhelper = anhelper;
        this.nameProviderByClass = nameProviderByClass;
        this.morphium = morphium;

        module = new SimpleModule();
        jackson = new com.fasterxml.jackson.databind.ObjectMapper();
        jackson.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jackson.setVisibility(jackson.getSerializationConfig()
                .getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

        module.addDeserializer(MorphiumId.class, new JsonDeserializer<MorphiumId>() {
            @Override
            public MorphiumId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
                try {
                    String id = jsonParser.getValueAsString().substring(9);
                    id = id.substring(0, id.length() - 1);
                    return new MorphiumId(id);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Collection<Object> getKnownPropertyNames() {
                Collection<Object> col = new ArrayList<>();
                col.add("_id");
                return col;

            }

        });
        module.setDeserializerModifier(new BeanDeserializerModifier() {
            @Override
            public List<BeanPropertyDefinition> updateProperties(DeserializationConfig config, BeanDescription beanDesc, List<BeanPropertyDefinition> propDefs) {

                if (anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Entity.class) || anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Embedded.class)) {
                    List<BeanPropertyDefinition> lst = new ArrayList<>();
                    for (BeanPropertyDefinition d : propDefs) {
                        Field fld = anhelper.getField(beanDesc.getBeanClass(), d.getName());
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
                if (beanDesc.getBeanClass().equals(BigInteger.class)) {
                    return new JsonDeserializer<Object>() {
                        @Override
                        public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
                            return new BigIntegerTypeMapper().unmarshall(jsonParser.readValueAs(Object.class));
                        }
                    };
                }

                if (List.class.isAssignableFrom(beanDesc.getBeanClass())) {
                    return new JsonDeserializer<List>() {
                        @Override
                        public List deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
                            List toAdd = new ArrayList();
                            List in = jsonParser.readValueAs(List.class);
                            for (Object e : in) {
                                if (e instanceof Map) {
                                    if (((Map) e).get("class_name") != null) {
                                        try {
                                            toAdd.add(jackson.convertValue(e, Class.forName((String) ((Map) e).get("class_name"))));
                                        } catch (ClassNotFoundException e1) {
                                            //TODO: Implement Handling
                                            throw new RuntimeException(e1);
                                        }
                                        continue;
                                    }
                                }
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
                            Map<String, String> m = (Map<String, String>) jsonParser.readValueAs(Map.class);
                            Class<Enum> target = null;
                            try {
                                target = (Class<Enum>) Class.forName(m.get("class_name"));
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }

                            return Enum.valueOf(target, m.get("name"));
                        }
                    };
                }
                return def;
            }
        });
        jackson.registerModule(module);
    }

    public <T> T unmarshall(Class<? extends T> theClass, Map<String, Object> o) {
        return jackson.convertValue(o, theClass);
    }

    private class EntityDeserializer extends JsonDeserializer<Object> {
        private final AnnotationAndReflectionHelper anhelper;
        private Logger log = LoggerFactory.getLogger(EntityDeserializer.class);
        private Class type;

        public EntityDeserializer(Class cls, AnnotationAndReflectionHelper an) {
            type = cls;
            anhelper = an;
        }

        @Override
        public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
            try {
                Object ret = null;
                try {
                    ret = type.newInstance();
                } catch (Exception ignored) {
                }
                if (ret == null) {
                    final Constructor<Object> constructor;
                    try {
                        constructor = (Constructor<Object>) reflection.newConstructorForSerialization(type, Object.class.getDeclaredConstructor());
                        ret = constructor.newInstance();
                    } catch (Exception e) {
                        log.error("Exception during instanciation of type " + type.getName(), e);
                    }
                }
                JsonToken tok = null;
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
                        List v = new ArrayList();
                        List o = jackson.readValue(jsonParser, List.class);
                        Class listElementType = null;
                        Type type = null;
                        if (f != null) {
                            type = f.getGenericType();
                            if (type != null) {
                                if (type instanceof ParameterizedType) {
                                    if (((ParameterizedType) type).getActualTypeArguments()[0] instanceof ParameterizedType) {
                                        //list of lists?
                                        //ParameterizedType t2= (ParameterizedType) ((ParameterizedType)type).getActualTypeArguments()[0];
                                        listElementType = List.class;

                                    } else {
                                        listElementType = (Class) ((ParameterizedType) type).getActualTypeArguments()[0];
                                    }
                                }
                            }
                        }
                        for (Object el : o) {
                            if (el instanceof Map) {
                                if (((Map) el).get("class_name") != null) {
                                    Class<?> cls = Class.forName((String) ((Map) el).get("class_name"));
                                    if (cls.isEnum()) {
                                        v.add(Enum.valueOf((Class) cls, (String) ((Map) el).get("name")));
                                    } else {
                                        v.add(jackson.convertValue(el, cls));
                                    }
                                } else {
                                    v.add(el);
                                }
                            } else if (listElementType != null) {
                                v.add(jackson.convertValue(el, listElementType));
                            } else {
                                v.add(el);
                            }

                        }
                        if (f != null) {
                            f.set(ret, v);
                        }
                        continue;

                    }
                    if (tok.equals(JsonToken.START_OBJECT)) {
                        ////////////////////
                        //object value
                        if (f.getType().isEnum()) {
                            Map v = jsonParser.readValueAs(Map.class);
                            f.set(ret, Enum.valueOf((Class) f.getType(), (String) v.get("name")));
                            continue;
                        }
                        if (f != null) {
                            Object v = jackson.readValue(jsonParser, f.getType());
                            f.set(ret, v);
                        } else {
                            //just read the value and ignore it
                            jackson.readValue(jsonParser, Object.class);
                        }

                        continue;
                    }


                    if (tok.equals(JsonToken.END_OBJECT)) {
                        return ret;
                    }

                    if (f == null) {
                        //ignore value
                        jackson.readValue(jsonParser, Object.class);
                        continue;
                    }

                    if (f.getType().isEnum()) {
                        Map m = jsonParser.readValueAs(Map.class);
                        f.set(ret, (Enum.valueOf((Class<Enum>) Class.forName((String) m.get("class_name")), (String) m.get("name"))));
                        continue;
                    }
                    if (Map.class.isAssignableFrom(f.getType())) {
                        Map m = jsonParser.readValueAs(Map.class);
                        Map res = new HashMap();
                        for (Map.Entry en : ((Map<Object, Object>) m).entrySet()) {
                            if (en.getValue() instanceof Map) {
                                String clsName = (String) ((Map) en.getValue()).get("class_name");
                                if (clsName != null) {
                                    Class<?> toValueType = Class.forName(clsName);
                                    if (toValueType.isEnum()) {
                                        res.put(en.getKey(), Enum.valueOf((Class<Enum>) toValueType, (String) ((Map) en.getValue()).get("name")));
                                    } else {
                                        res.put(en.getKey(), jackson.convertValue(en.getValue(), toValueType));
                                    }
                                    continue;
                                }
                            }
                            res.put(en.getKey(), en.getValue());
                        }
                        f.set(ret, res);
                        continue;
                    }


                    //todo: list of references!
                    Reference r = f.getAnnotation(Reference.class);
                    if (r != null) {
                        MorphiumReference ref = jackson.readValue(jsonParser, MorphiumReference.class);
                        if (r.lazyLoading()) {
                            f.set(ret, morphium.createLazyLoadedEntity(f.getType(), ref.getId(), ref.getCollectionName()));
                        } else {
                            f.set(ret, morphium.findById(f.getType(), ref.getId()));
                        }
                        continue;
                    }
                    f.set(ret, jackson.readValue(jsonParser, f.getType()));

                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }
}