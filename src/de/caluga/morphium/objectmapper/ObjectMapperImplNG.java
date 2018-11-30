package de.caluga.morphium.objectmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumObjectMapper;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.mapping.BigIntegerTypeMapper;
import de.caluga.morphium.mapping.MorphiumTypeMapper;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
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
    private final Map<Class<?>, NameProvider> nameProviderByClass;

    private final JSONParser jsonParser = new JSONParser();

    private Logger log = LoggerFactory.getLogger(ObjectMapperImplNG.class);
    private MorphiumSerializer marshaller;
    private MorphiumDeserializer unmarshaller;
    private final Map<Class, MorphiumTypeMapper> typeMappers;

    public ObjectMapperImplNG() {
        nameProviderByClass = new ConcurrentHashMap<>();
        //noinspection unchecked
        typeMappers = new ConcurrentHashMap();
        typeMappers.put(BigInteger.class, new BigIntegerTypeMapper());

        ContainerFactory containerFactory = new ContainerFactory() {
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
            marshaller = new MorphiumSerializer(anhelper, nameProviderByClass, morphium, this, typeMappers);
        }
        return marshaller;
    }


    public MorphiumDeserializer getDeserializer() {
        if (unmarshaller == null) {
            unmarshaller = new MorphiumDeserializer(anhelper, nameProviderByClass, morphium, typeMappers);
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
        if (o == null) return null;
        if (o.containsKey("class_name")) {
            try {
                //noinspection unchecked
                theClass = anhelper.getClassForTypeId(o.get("class_name").toString());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not get class", e);
            }
        }
        return getDeserializer().deserialize(theClass, o);
    }

    @Override
    public <T> T deserialize(Class<? extends T> cls, String json) throws IOException {

        Map obj = new ObjectMapper().readValue(json, Map.class); //(HashMap<String, Object>) jsonParser.parse(json, containerFactory);
        if (obj.containsKey("class_name")) {
            try {
                //noinspection unchecked
                cls = anhelper.getClassForTypeId(obj.get("class_name").toString());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not get class", e);
            }
        }
        //noinspection unchecked,unchecked
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

    public Map<Class, MorphiumTypeMapper> getTypeMappers() {
        return typeMappers;
    }

    @Override
    public <T> void registerCustomMapperFor(Class<T> cls, MorphiumTypeMapper<T> map) {
        typeMappers.put(cls, map);
    }

    @Override
    public void deregisterCustomMapperFor(Class cls) {
        typeMappers.remove(cls);
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
