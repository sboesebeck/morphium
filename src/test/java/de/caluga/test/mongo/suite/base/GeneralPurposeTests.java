package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.SimpleEntity;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


@Tag("core")
public class GeneralPurposeTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void mapQueryTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Query<Map<String, Object>> q = morphium.createMapQuery(morphium.getMapper().getCollectionName(UncachedObject.class));
            List<Map<String, Object>> lst = q.asMapList();
            assertEquals(100, lst.size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void mapQueryTestIterable(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Query<Map<String, Object>> q = morphium.createMapQuery(morphium.getMapper().getCollectionName(UncachedObject.class));
            int count = 0;
            for (Map<String, Object> o : q.asMapIterable()) {
                count++;
                assertNotNull(q);

            }
            assertEquals(100, count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void simpleEntityTest(Morphium morphium) throws Exception {
        try (morphium) {
            SimpleEntity<String> e = new SimpleEntity<>();
            e.setId("test1");
            e.put("ts", System.currentTimeMillis());
            e.put("name", "a name");
            e.put("something", new UncachedObject("test", 12));
            morphium.store(e);

            Thread.sleep(100);
            e = morphium.reread(e);
            assertNotNull(e);
            assertEquals("test1", e.getId());
            assertTrue(((Long) e.get("ts")) > 0);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void simpleEntityQueryTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 0; i < 100; i++) {
                SimpleEntity<MorphiumId> simple = new SimpleEntity<>();
                simple.put("counter_simple", i);
                simple.put("value", "V: " + i);
                morphium.store(simple);
            }

            long count = morphium.createQueryFor(SimpleEntity.class).countAll();
            assertEquals(100, count);

            List<SimpleEntity> lst = morphium.createQueryFor(SimpleEntity.class).f("counter_simple").eq(42).asList();
            assertEquals(1, lst.size());
            assertEquals(42, lst.get(0).get("counter_simple"));
        }
    }
}
