package de.caluga.test.objectmapping;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ObjectMapperTest {
    private Logger log = LoggerFactory.getLogger(ObjectMapperTest.class);

    @Test
    public void testObjectMapper() throws Exception {

        MorphiumObjectMapper om = new ObjectMapperImpl();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        om.setAnnotationHelper(an);

//        ObjectMapper jacksonOM = new ObjectMapper();

        UncachedObject uc = new UncachedObject("strValue", 144);
        uc.setBinaryData("Test".getBytes(StandardCharsets.UTF_8));

        long start = System.currentTimeMillis();
        int amount = 10000;
        for (int i = 0; i < amount; i++) {
            Map<String, Object> m = om.serialize(uc);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Serialization took " + dur + " ms");


//        start = System.currentTimeMillis();
//        for (int i = 0; i < 1000; i++) {
//            jacksonOM.convertValue(uc, Map.class);
//        }
//        dur = System.currentTimeMillis() - start;
//        log.info("Jackson serialization took " + dur + " ms");


        Map<String, Object> m = om.serialize(uc);
        start = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            UncachedObject uo = om.deserialize(UncachedObject.class, m);
        }
        dur = System.currentTimeMillis() - start;

        log.info("De-Serialization took " + dur + " ms");


//        Map<String,Object> convNames=new HashMap<>();
//        for (Map.Entry<String,Object> e:m.entrySet()){
//            Field f = an.getField(UncachedObject.class,e.getKey());
//            convNames.put(f.getName(),e.getValue());
//        }
//        start = System.currentTimeMillis();
//        for (int i = 0; i < 1000; i++) {
//            UncachedObject uo = jacksonOM.convertValue(convNames,UncachedObject.class);
//        }
//        dur = System.currentTimeMillis() - start;
//
//        log.info("Jackson De-Serialization took " + dur + " ms");

    }


}
