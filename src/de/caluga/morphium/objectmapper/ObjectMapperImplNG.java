package de.caluga.morphium.objectmapper;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.ObjectMapper;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ObjectMapperImplNG implements ObjectMapper {
    private Morphium morphium;
    private AnnotationAndReflectionHelper anhelper;
    private Map<Class<?>, NameProvider> nameProviderByClass;

    private final ContainerFactory containerFactory;
    private final JSONParser jsonParser = new JSONParser();


    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;

    private Logger log = LoggerFactory.getLogger(ObjectMapperImplNG.class);
    private MarshallHelper marshaller;
    private UnmarshallHelper unmarshaller;

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

    public MarshallHelper getMarshaller() {
        if (marshaller == null) {
            marshaller = new MarshallHelper(anhelper, nameProviderByClass, morphium, this);
        }
        return marshaller;
    }


    public UnmarshallHelper getUnmarshaller() {
        if (unmarshaller == null) {
            unmarshaller = new UnmarshallHelper(anhelper, nameProviderByClass, morphium, this);
        }
        return unmarshaller;
    }



    @Override
    public Map<String, Object> marshall(Object o) {
        return getMarshaller().marshall(o);
    }

    @Override
    public String getCollectionName(Class cls) {
        return getMarshaller().getCollectionName(cls);
    }

    @Override
    public Object marshallIfNecessary(Object o) {
        return getMarshaller().marshallIfNecessary(o);
    }


    @Override
    public <T> T unmarshall(Class<? extends T> theClass, Map<String, Object> o) {
        return getUnmarshaller().unmarshall(theClass, o);
    }

    @Override
    public <T> T unmarshall(Class<? extends T> cls, String json) throws ParseException {

        HashMap<String, Object> obj = (HashMap<String, Object>) jsonParser.parse(json, containerFactory);
        return getUnmarshaller().unmarshall(cls, obj);

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

    @Override
    public void setAnnotationHelper(AnnotationAndReflectionHelper an) {
        anhelper = an;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

}
