package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.ReflectionFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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
                        ((POJOPropertyBuilder) def).addField(d.getField(), pn, true, true, false);
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
                if (beanDesc.getBeanClass().isEnum()) {
                    return new JsonDeserializer<Enum>() {
                        @Override
                        public Enum deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
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
        public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String l = null;
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
                while ((l = jsonParser.nextFieldName()) != null) {
                    JsonToken t = jsonParser.nextValue();
//                    log.info("Field " + l);
//                    log.info("value " + jsonParser.getValueAsString());

                    Field f = anhelper.getField(type, l);

                    if (f == null) {
                        //log.error("Could not find field " + l + " in type " + type.getName());
                        continue;
                    }
                    f.setAccessible(true);
                    if (List.class.isAssignableFrom(f.getType())) {
                        List lst = jackson.readValue(jsonParser, List.class);
                        List res = new ArrayList();

                        for (Object el : lst) {
                            if (el instanceof Map) {
                                Map elem = (Map) el;
                                if (elem.get("class_name") != null) {
                                    Class cls = Class.forName((String) elem.get("class_name"));
                                    if (cls.isEnum()) {
                                        res.add(Enum.valueOf(cls, (String) elem.get("name")));
                                    } else {
                                        res.add(jackson.convertValue(elem, cls));
                                    }
                                }
                            } else {
                                res.add(el);
                            }
                        }
                        f.set(ret, res);
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
                return ret;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }
}