package de.caluga.test.objectmapping;


import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.base.BasicFunctionalityTest.ListOfIdsContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;

public class ObjectMapperTest {
    private Logger log = LoggerFactory.getLogger(ObjectMapperTest.class);

    @Test
    public void marshallListOfIdsTest() {
        ListOfIdsContainer c = new ListOfIdsContainer();
        c.others = new ArrayList<>();
        c.others.add(new MorphiumId());
        c.others.add(new MorphiumId());
        c.others.add(new MorphiumId());
        c.others.add(new MorphiumId());
        c.simpleId = new MorphiumId();

        c.idMap = new HashMap<>();
        c.idMap.put("1", new MorphiumId());
        MorphiumObjectMapper mapper=new ObjectMapperImpl();
        Map<String, Object> marshall = mapper.serialize(c);
        assert (marshall.get("simple_id") instanceof ObjectId);
        assert (((Map) marshall.get("id_map")).get("1") instanceof ObjectId);
        for (Object i : (List) marshall.get("others")) {
            assert (i instanceof ObjectId);
        }

        ///

        c = mapper.deserialize(ListOfIdsContainer.class, marshall);
        //noinspection ConstantConditions
        assert (c.idMap != null && c.idMap.get("1") != null && c.idMap.get("1") instanceof MorphiumId);
        //noinspection ConstantConditions
        assert (c.others.size() == 4 && c.others.get(0) instanceof MorphiumId);
        assertNotNull(c.simpleId);
        ;
    }

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