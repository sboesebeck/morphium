package de.caluga.test.objectmapping;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
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
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.base.BasicFunctionalityTest.ListOfIdsContainer;
import de.caluga.test.mongo.suite.base.ObjectMapperImplTest;
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
        MorphiumObjectMapper mapper = new ObjectMapperImpl();
        Map<String, Object> marshall = mapper.serialize(c);
        assert(marshall.get("simple_id") instanceof ObjectId);
        assert(((Map<?, ?>) marshall.get("id_map")).get("1") instanceof ObjectId);

        for (Object i : (List<?>) marshall.get("others")) {
            assert(i instanceof ObjectId);
        }

        ///
        c = mapper.deserialize(ListOfIdsContainer.class, marshall);
        // noinspection ConstantConditions
        assert(c.idMap != null && c.idMap.get("1") != null && c.idMap.get("1") instanceof MorphiumId);
        // noinspection ConstantConditions
        assert(c.others.size() == 4 && c.others.get(0) instanceof MorphiumId);
        assertNotNull(c.simpleId);
        ;
    }

    @Test
    public void serializeMsg() throws Exception {
        var om = new ObjectMapperImpl();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        om.setAnnotationHelper(an);
        Msg m = new Msg("test1", "test2", "test3");
        var map = om.serialize(m);
        log.info(Utils.toJsonString(map));
        assertNotNull(map.get("class_name"));
    }

    @Test
    public void mapSerializationTest() {
        var OM = new ObjectMapperImpl();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        OM.setAnnotationHelper(an);
        Map<String, Object> map = OM.serialize(new ObjectMapperImplTest.Simple());
        log.info("Got map");
        assert(map.get("test").toString().startsWith("test"));
        ObjectMapperImplTest.Simple s = OM.deserialize(ObjectMapperImplTest.Simple.class, map);
        log.info("Got simple");
        Map<String, Object> m = new HashMap<>();
        m.put("test", "testvalue");
        m.put("simple", s);
        map = OM.serializeMap(m, null);
        assert(map.get("test").equals("testvalue"));
        List<ObjectMapperImplTest.Simple> lst = new ArrayList<>();
        lst.add(new ObjectMapperImplTest.Simple());
        lst.add(new ObjectMapperImplTest.Simple());
        lst.add(new ObjectMapperImplTest.Simple());
        List<Object> serializedList = OM.serializeIterable(lst, null, null);
        assert(serializedList.size() == 3);
        List<ObjectMapperImplTest.Simple> deserializedList = OM.deserializeList(serializedList);
        log.info("Deserialized " + deserializedList.size());
    }

    @Test
    public void testObjectMapper() throws Exception {
        MorphiumObjectMapper om = new ObjectMapperImpl();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        om.setAnnotationHelper(an);
        ObjectMapper jacksonOM = new ObjectMapper();
        UncachedObject uc = new UncachedObject("strValue", 144);
        uc.setBinaryData("Test".getBytes(StandardCharsets.UTF_8));
        long start = System.currentTimeMillis();
        int amount = 100000;

        for (int i = 0; i < amount; i++) {
            Map<String, Object> m = om.serialize(uc);
            assertNotNull(m);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("Morphium Serialization took " + dur + " ms");
        start = System.currentTimeMillis();

        for (int i = 0; i < amount; i++) {
            var m=jacksonOM.convertValue(uc, Map.class);
            assertNotNull(m);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Jackson serialization took " + dur + " ms");
        Map<String, Object> m = om.serialize(uc);
        start = System.currentTimeMillis();

        for (int i = 0; i < amount; i++) {
            UncachedObject uo = om.deserialize(UncachedObject.class, m);
            assertNotNull(uo);
        }

        dur = System.currentTimeMillis() - start;
        log.info("De-Serialization took " + dur + " ms");
        Map<String, Object> convNames = new HashMap<>();

        for (Map.Entry<String, Object> e : m.entrySet()) {
            Field f = an.getField(UncachedObject.class, e.getKey());
            convNames.put(f.getName(), e.getValue());
        }

        start = System.currentTimeMillis();

        for (int i = 0; i < amount; i++) {
            UncachedObject uo = jacksonOM.convertValue(convNames, UncachedObject.class);
            assertNotNull(uo);
        }

        dur = System.currentTimeMillis() - start;
        log.info("Jackson De-Serialization took " + dur + " ms");
    }

}
