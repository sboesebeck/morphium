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
import java.lang.reflect.*;
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
                    String n = jsonParser.nextFieldName();
                    n = jsonParser.nextFieldName();
                    String valueAsString = jsonParser.getValueAsString();
                    jsonParser.nextToken();
                    if (valueAsString == null) return null;
                    return new MorphiumId(valueAsString);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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
        Object ret = replaceMorphiumIds(o);
        return jackson.convertValue(ret, theClass);
    }

    private Object replaceMorphiumIds(Object o) {
        if (o instanceof Map) {
            return replaceMorphiumIds((Map) o);
        }
        return o;
    }

    private Object replaceMorphiumIds(Map<String, Object> m) {
        if (m.get("morphium id") != null) {
            return new MorphiumId((String) m.get("morphium id"));
        }
        Map ret = new HashMap();
        for (Map.Entry e : m.entrySet()) {
            if (e.getValue() instanceof Map) {
                ret.put(e.getKey(), replaceMorphiumIds((Map<String, Object>) e.getValue()));
            } else if (e.getValue() instanceof List) {
                ret.put(e.getKey(), replaceMorphiumIds((List) e.getValue()));
            } else if (e.getValue() instanceof MorphiumId) {
                Map o = new LinkedHashMap();
                o.put("morphium id", e.getValue().toString());
                ret.put(e.getKey(), o);
            } else {
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
                m.put("morphium id", o.toString());
                ret.add(m);
            } else if (o instanceof Map) {
                ret.add(replaceMorphiumIds((Map) o));
            } else if (o instanceof List) {
                ret.add(replaceMorphiumIds((List) o));
            } else {
                ret.add(o);
            }
        }
        return ret;
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
                        List o = jackson.readValue(jsonParser, List.class);
                        f.set(ret, handleList(f.getGenericType(), o));
                        continue;

                    }
                    if (tok.equals(JsonToken.START_OBJECT)) {
                        ////////////////////
                        //object value
                        if (f != null && f.getType().isEnum()) {
                            Map v = jsonParser.readValueAs(Map.class);
                            f.set(ret, Enum.valueOf((Class) f.getType(), (String) v.get("name")));
                            continue;
                        }
                        if (f != null) {
                            //todo: list of references!
                            Reference r = f.getAnnotation(Reference.class);
                            if (r != null) {
                                MorphiumReference ref = jackson.readValue(jsonParser, MorphiumReference.class);
                                if (r.lazyLoading()) {
                                    f.set(ret, morphium.createLazyLoadedEntity(f.getType(), ref.getId(), ref.getCollectionName()));
                                } else {
                                    Object id = replaceMorphiumIds(ref.getId());
                                    Object refObj = morphium.findById(f.getType(), id);
                                    f.set(ret, refObj);
                                }
                                continue;
                            }
                            if (Map.class.isAssignableFrom(f.getType())) {
                                Map m = (Map) jackson.readValue(jsonParser, f.getType());
                                Map v = handleMap(m);
                                f.set(ret, v);
                            } else {
                                if (f.getType().equals(String.class)) {
                                    f.set(ret, jackson.readValue(jsonParser, Object.class).toString());
                                } else {
                                    Object v = jackson.readValue(jsonParser, f.getType());
                                    f.set(ret, v);
                                }
                            }
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
//                        }
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
    }

    private Map handleMap(Map m) throws ClassNotFoundException {
        Map v = new LinkedHashMap();
        for (Map.Entry e : (Set<Map.Entry>) m.entrySet()) {
            if (e.getValue() instanceof Map) {
                //Object? Enum? in a Map... Map<Something,MAP>
                Object toPut = null;

                Map ev = (Map) e.getValue();
                Class cls = Map.class;
                if (ev.get("class_name") != null) {
                    cls = Class.forName((String) ev.get("class_name"));
                    if (cls.isEnum()) {
                        toPut = Enum.valueOf(cls, (String) ev.get("name"));
                    } else {
                        toPut = jackson.convertValue(ev, cls);
                    }
                } else if (ev.get("morphium id") != null) {
                    toPut = new MorphiumId((String) ev.get("morphium id"));
                } else {
                    toPut = handleMap(ev);
                }
                v.put(e.getKey(), toPut);
                continue;

            } else if (e.getValue() instanceof List) {
                ///Map<Something,List>
                Object retLst = null;
                    retLst = handleList(List.class, (List) e.getValue());
                v.put(e.getKey(), retLst);
            } else {
                v.put(e.getKey(), e.getValue());
            }
        }
        return v;
    }

    private Object handleList(Type type, List listIn) throws ClassNotFoundException {
        Class listElementType = null;
        List listOut = new ArrayList();
        if (type != null) {
            if (type instanceof ParameterizedType) {
                if (((ParameterizedType) type).getActualTypeArguments()[0] instanceof ParameterizedType) {
                    //list of lists? list of maps?
                    //ParameterizedType t2= (ParameterizedType) ((ParameterizedType)type).getActualTypeArguments()[0];
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
                } else if (((Map) el).get("class_name") != null) {
                    Class<?> cls = Class.forName((String) ((Map) el).get("class_name"));
                    if (cls.isEnum()) {
                        listOut.add(Enum.valueOf((Class) cls, (String) ((Map) el).get("name")));
                    } else {
                        listOut.add(jackson.convertValue(el, cls));
                    }
                } else {
                    if (listElementType != null) {
                        listOut.add(jackson.convertValue(el, listElementType));
                    } else {
                        listOut.add(handleMap((Map) el));
                    }
                }
            } else if (el instanceof List) {
                if (type instanceof ParameterizedType) {
                    listOut.add(handleList(((ParameterizedType) type).getActualTypeArguments()[0], (List) el));
                } else {
                    listOut.add(handleList(null, (List) el));
                }
            } else if (listElementType != null) {
                listOut.add(jackson.convertValue(el, listElementType));
            } else {
                listOut.add(el);
            }

        }
        if (type != null && type instanceof Class && ((Class) type).isArray()) {
            Object arr = Array.newInstance(((Class) type).getComponentType(), listOut.size());
            for (int i = 0; i < listOut.size(); i++) {
                if (((Class) type).getComponentType().isPrimitive()) {
                    if (((Class) type).getComponentType().equals(int.class)) {
                        if (listOut.get(i) instanceof Double) {
                            Array.set(arr, i, ((Double) listOut.get(i)).intValue());
                        } else if (listOut.get(i) instanceof Integer) {
                            Array.set(arr, i, listOut.get(i));
                        } else if (listOut.get(i) instanceof Long) {
                            Array.set(arr, i, ((Long) listOut.get(i)).intValue());
                        } else {
                            //noinspection RedundantCast
                            Array.set(arr, i, listOut.get(i));
                        }

                    } else if (((Class) type).getComponentType().equals(long.class)) {
                        if (listOut.get(i) instanceof Double) {
                            Array.set(arr, i, ((Double) listOut.get(i)).longValue());
                        } else if (listOut.get(i) instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOut.get(i)).longValue());
                        } else if (listOut.get(i) instanceof Long) {
                            Array.set(arr, i, listOut.get(i));
                        } else {
                            Array.set(arr, i, listOut.get(i));
                        }

                    } else if (((Class) type).getComponentType().equals(float.class)) {
                        //Driver sends doubles instead of floats
                        if (listOut.get(i) instanceof Double) {
                            Array.set(arr, i, ((Double) listOut.get(i)).floatValue());
                        } else if (listOut.get(i) instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOut.get(i)).floatValue());
                        } else if (listOut.get(i) instanceof Long) {
                            Array.set(arr, i, ((Long) listOut.get(i)).floatValue());
                        } else {
                            Array.set(arr, i, listOut.get(i));
                        }

                    } else if (((Class) type).getComponentType().equals(double.class)) {
                        if (listOut.get(i) instanceof Float) {
                            Array.set(arr, i, ((Float) listOut.get(i)).doubleValue());
                        } else if (listOut.get(i) instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOut.get(i)).doubleValue());
                        } else if (listOut.get(i) instanceof Long) {
                            Array.set(arr, i, ((Long) listOut.get(i)).doubleValue());
                        } else {
                            Array.set(arr, i, listOut.get(i));
                        }

                    } else if (((Class) type).getComponentType().equals(byte.class)) {
                        if (listOut.get(i) instanceof Integer) {
                            Array.set(arr, i, ((Integer) listOut.get(i)).byteValue());
                        } else if (listOut.get(i) instanceof Long) {
                            Array.set(arr, i, ((Long) listOut.get(i)).byteValue());
                        } else {
                            Array.set(arr, i, listOut.get(i));
                        }
                    } else if (((Class) type).getComponentType().equals(boolean.class)) {
                        if (listOut.get(i) instanceof String) {
                            Array.set(arr, i, listOut.get(i).toString().equalsIgnoreCase("true"));
                        } else if (listOut.get(i) instanceof Integer) {
                            Array.set(arr, i, (Integer) listOut.get(i) == 1);
                        } else {
                            Array.set(arr, i, listOut.get(i));
                        }

                    }
                } else {
                    Array.set(arr, i, listOut.get(i));
                }
            }
            return arr;
        }

        return listOut;
    }
}