package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.ObjectMapper;
import de.caluga.morphium.driver.MorphiumId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.ReflectionFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class MorphiumDeserializer {

    private final AnnotationAndReflectionHelper anhelper;
    private final Map<Class<?>, NameProvider> nameProviderByClass;
    private final Morphium morphium;
    private final ObjectMapper objectMapper;

    private final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();
    private final Logger log = LoggerFactory.getLogger(MorphiumSerializer.class);
    private final SimpleModule module;
    private final com.fasterxml.jackson.databind.ObjectMapper jackson;
    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;

    public MorphiumDeserializer(AnnotationAndReflectionHelper anhelper, Map<Class<?>, NameProvider> nameProviderByClass, Morphium morphium, ObjectMapper objectMapper) {

        this.anhelper = anhelper;
        this.nameProviderByClass = nameProviderByClass;
        this.morphium = morphium;

        this.objectMapper = objectMapper;
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

                return null;
            }

            @Override
            public Collection<Object> getKnownPropertyNames() {
                Collection<Object> col = new ArrayList<>();
                col.add("_id");
                return col;

            }

        });
        jackson.registerModule(module);
    }

    public <T> T unmarshall(Class<? extends T> theClass, Map<String, Object> o) {
        return jackson.convertValue(o, theClass);
    }
}