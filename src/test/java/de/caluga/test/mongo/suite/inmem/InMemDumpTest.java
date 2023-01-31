package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class InMemDumpTest extends MorphiumInMemTestBase {

    @Test
    public void jsonDumpTest() throws Exception {

        MorphiumTypeMapper<ObjectId> mapper = new MorphiumTypeMapper<ObjectId>() {
            @Override
            public Object marshall(ObjectId o) {
                Map<String, String> m = new HashMap<>();
                m.put("value", o.toHexString());
                m.put("class_name", o.getClass().getName());
                return m;

            }

            @Override
            public ObjectId unmarshall(Object d) {
                return new ObjectId(((Map) d).get("value").toString());
            }
        };
        morphium.getMapper().registerCustomMapperFor(ObjectId.class, mapper);
        for (int i = 0; i < 10; i++) {
            UncachedObject e = new UncachedObject();
            e.setCounter(i);
            e.setStrValue("value" + i);
            morphium.store(e);
        }
        ExportContainer cnt = new ExportContainer();
        cnt.created = System.currentTimeMillis();

//        cnt.data = ((InMemoryDriver) morphium.getDriver()).getDatabase(morphium.getDriver().listDatabases().get(0));
//
//        Map<String, Object> s = morphium.getMapper().serialize(cnt);
//        System.out.println(Utils.toJsonString(s));
//
//        morphium.dropCollection(UncachedObject.class);
//        ExportContainer ex = morphium.getMapper().deserialize(ExportContainer.class, Utils.toJsonString(s));
//        assertNotNull(ex);;
//        ((InMemoryDriver) morphium.getDriver()).setDatabase(morphium.getDriver().listDatabases().get(0), ex.data);
//
//        List<UncachedObject> result = morphium.createQueryFor(UncachedObject.class).asList();
//        assert (result.size() == 10);
//        assert (result.get(1).getCounter() == 1);
    }


    @Test
    public void driverDumpTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject e = new UncachedObject();
            e.setCounter(i);
            e.setStrValue("value" + i);
            e.setIntData(new int[]{i, i + 1, i + 2});
            e.setDval(42.00001);
            e.setBinaryData(new byte[]{1, 2, 3, 4, 5});
            morphium.store(e);

            ComplexObject o = new ComplexObject();
            o.setEinText("A text " + i);
            o.setEmbed(new EmbeddedObject("emb", "v1", System.currentTimeMillis()));
            o.setRef(e);
            morphium.store(o);


        }

        ByteArrayOutputStream bout = new ByteArrayOutputStream();

//        InMemoryDriver driver = (InMemoryDriver) morphium.getDriver();
//        driver.dump(morphium, morphium.getDriver().listDatabases().get(0), bout);
//        log.info("database dump is " + bout.size());
//
//        driver.close();
//        driver.connect();
//        driver.restore(new ByteArrayInputStream(bout.toByteArray()));
        assertEquals (100,TestUtils.countUC(morphium));
        assertEquals(100,morphium.createQueryFor(ComplexObject.class).countAll());

        for (ComplexObject co : morphium.createQueryFor(ComplexObject.class).asList()) {
            assertNotNull(co.getEinText());
            assertNotNull(co.getRef());
        }
    }

    @Entity
    public static class ExportContainer {
        @Id
        public Long created;
        public Map<String, List<Map<String, Object>>> data;
    }
}
