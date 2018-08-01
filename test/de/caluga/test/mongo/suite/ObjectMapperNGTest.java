package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapper.ObjectMapperImplNG;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ObjectMapperNGTest extends MongoTest {

    @Test
    public void simpleTest() {
        ObjectMapperImplNG map = new ObjectMapperImplNG();
        map.setMorphium(morphium);
        map.setAnnotationHelper(new AnnotationAndReflectionHelper(true));

        UncachedObject o = new UncachedObject("Value", 1234);
        o.setMorphiumId(new MorphiumId());
        o.setBinaryData(new byte[]{12, 2, 3, 4, 5, 55, 5, 3});

        Map obj = map.serialize(o);
        assert (obj != null);
        assert (obj.get("value").equals("Value"));

        UncachedObject o2 = map.deserialize(UncachedObject.class, obj);
        assert (o2 != null);
        assert (o2.getMorphiumId() != null);

    }

    @Test
    public void complexTest() {
        morphium.getConfig().setReplicasetMonitoring(false);
        ObjectMapperImplNG map = new ObjectMapperImplNG();
        map.setMorphium(morphium);
        map.setAnnotationHelper(new AnnotationAndReflectionHelper(true));

        ComplexObject co = new ComplexObject();
        co.setId(new MorphiumId());
        co.setEinText("The text");
        co.setEmbed(new EmbeddedObject("embb", "val", System.currentTimeMillis()));
        co.setcRef(new CachedObject("valuec", 1));
        co.setRef(new UncachedObject("value uc", 2));
        Map<String, Object> obj = map.serialize(co);
        assert (obj != null);
        assert (obj.get("embed") instanceof Map);
        assert (((Map) obj.get("embed")).get("name").equals("embb"));

        ComplexObject co2 = map.deserialize(ComplexObject.class, obj);
        assert (co2 != null);

        co = new ComplexObject();
        co.setId(new MorphiumId());
        co.setEinText("The text");
        co.setEmbed(new EmbeddedObject("embb", "val", System.currentTimeMillis()));
        co.setcRef(new CachedObject("valuec", 1));
        co.setRef(new UncachedObject("value uc", 2));
        co.setEmbeddedObjectList(new ArrayList<>());
        co.getEmbeddedObjectList().add(new EmbeddedObject("test", "valu3e", System.currentTimeMillis()));
        obj = map.serialize(co);
        assert (obj.get("embeddedObjectList") instanceof List);

        co2 = map.deserialize(ComplexObject.class, obj);
        assert (co2 != null);
    }


}
