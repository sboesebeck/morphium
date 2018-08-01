package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumObjectMapper;
import de.caluga.morphium.NameProvider;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 17.07.18
 * Time: 20:45
 * <p>
 * TODO: Add documentation here
 */
public class ObjectMapperImplNG implements MorphiumObjectMapper {
    private Morphium morphium;
    private AnnotationAndReflectionHelper anhelper;
    private Map<Class<?>, NameProvider> nameProviderByClass;

    private final ContainerFactory containerFactory;
    private final JSONParser jsonParser = new JSONParser();

    private Logger log = LoggerFactory.getLogger(ObjectMapperImplNG.class);
    private MorphiumSerializer marshaller;
    private MorphiumDeserializer unmarshaller;

    public ObjectMapperImplNG() {
        nameProviderByClass = new ConcurrentHashMap<>();

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

        anhelper = new AnnotationAndReflectionHelper(true); //default
    }

    public MorphiumSerializer getSerializer() {
        if (marshaller == null) {
            marshaller = new MorphiumSerializer(anhelper, nameProviderByClass, morphium, this);
        }
        return marshaller;
    }


    public MorphiumDeserializer getDeserializer() {
        if (unmarshaller == null) {
            unmarshaller = new MorphiumDeserializer(anhelper, nameProviderByClass, morphium, this);
        }
        return unmarshaller;
    }



    @Override
    public Map<String, Object> serialize(Object o) {
        return getSerializer().serialize(o);
    }

    @Override
    public String getCollectionName(Class cls) {
        return getSerializer().getCollectionName(cls);
    }

    @Override
    public <T> T deserialize(Class<? extends T> theClass, Map<String, Object> o) {
        return getDeserializer().deserialize(theClass, o);
    }

    @Override
    public <T> T deserialize(Class<? extends T> cls, String json) throws IOException {

        Map obj = new ObjectMapper().readValue(json, Map.class); //(HashMap<String, Object>) jsonParser.parse(json, containerFactory);
        return (T) getDeserializer().deserialize(cls, obj);

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

    public <T> void registerCustomTypeMapper(Class<T> cls, JsonSerializer<T> serializer) {
        getSerializer().registerTypeMapper(cls, serializer);
    }

    public <T> void deregisterCustomTypeMapperFor(Class<T> cls) {
        getSerializer().deregisterTypeMapperFor(cls);
    }

    @Override
    public void setAnnotationHelper(AnnotationAndReflectionHelper an) {
        anhelper = an;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

}
