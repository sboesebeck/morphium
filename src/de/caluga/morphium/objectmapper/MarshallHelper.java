package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.ObjectMapper;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MarshallHelper {

    private final List<Class> mongoTypes;
    private final AnnotationAndReflectionHelper anhelper;
    private final Map<Class<?>, NameProvider> nameProviderByClass;
    private final Morphium morphium;
    private final ObjectMapper objectMapper;
    private final Logger log = LoggerFactory.getLogger(MarshallHelper.class);
    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;
    private final com.fasterxml.jackson.databind.ObjectMapper jackson;

    public MarshallHelper(AnnotationAndReflectionHelper ar, Map<Class<?>, NameProvider> np, Morphium m, ObjectMapper om) {
        mongoTypes = Collections.synchronizedList(new ArrayList<>());
        anhelper = ar;
        nameProviderByClass = np;
        morphium = m;

        objectMapper = om;
        SimpleModule s = new SimpleModule();


        s.addSerializer(MorphiumId.class, new JsonSerializer<MorphiumId>() {
            @Override
            public void serialize(MorphiumId morphiumId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
                jsonGenerator.writeString("ObjectId(" + morphiumId.toString() + ")");
            }
        });

//        ScanResult res = new FastClasspathScanner("").scan();
//
//        for (String n:res.getNamesOfClassesWithAnnotation(Entity.class)){
//            log.info("Found Entity: "+n);
//            Class<?> cls = res.classNameToClassRef(n);
//
//        }
        s.setSerializerModifier(new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
                if (anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Entity.class) || anhelper.isAnnotationPresentInHierarchy(beanDesc.getBeanClass(), Embedded.class)) {
                    return new EntitySerializer((JsonSerializer<Object>) serializer, anhelper);
                }
                return serializer;
            }
        });
        jackson = new com.fasterxml.jackson.databind.ObjectMapper();
        jackson.registerModule(s);
    }


    public Map<String, Object> marshall(Object o) {

        Map m = jackson.convertValue(o, Map.class);

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


    public Object marshallIfNecessary(Object o) {
        return null;
    }

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

            for (Field fld : an.getAllFields(o.getClass())) {
                try {
                    fld.setAccessible(true);
                    String fldName = an.getFieldName(o.getClass(), fld.getName());
                    jsonGenerator.writeObjectField(fldName, fld.get(o));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            jsonGenerator.writeEndObject();
        }

    }
}
