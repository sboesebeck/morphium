package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.objectmapper.ObjectMapperImplNG;
import org.junit.Test;

import java.util.Map;

public class ObjectMapperNGTest {

    @Test
    public void simpleTest() throws Exception {
        ObjectMapperImplNG map = new ObjectMapperImplNG();
        map.setAnnotationHelper(new AnnotationAndReflectionHelper(true));

        UncachedObject o = new UncachedObject("Value", 1234);
        o.setBinaryData(new byte[]{12, 2, 3, 4, 5, 55, 5, 3});

        Map obj = map.marshall(o);
        assert (obj != null);

        ComplexObject co = new ComplexObject();
        co.setEinText("The text");
        co.setEmbed(new EmbeddedObject("embb", "val", System.currentTimeMillis()));
        obj = map.marshall(co);
        assert (obj != null);

    }

}
