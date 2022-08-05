package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.SimpleEntity;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


public class GeneralPurposeTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void mapQueryTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Query<Map<String, Object>> q = morphium.createMapQuery(morphium.getMapper().getCollectionName(UncachedObject.class));
            List<Map<String, Object>> lst = q.asMapList();
            assertThat(lst.size()).isEqualTo(100);
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
                assertThat(q != null).isTrue();

            }
            assertThat(count).isEqualTo(100);
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
            assertThat(e).isNotNull();
            assertThat(e.getId()).isEqualTo("test1");
            assertThat(((Long) e.get("ts"))).isGreaterThan(0);
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
            assertThat(count).isEqualTo(100);

            List<SimpleEntity> lst = morphium.createQueryFor(SimpleEntity.class).f("counter_simple").eq(42).asList();
            assertThat(lst.size()).isEqualTo(1);
            assertThat(lst.get(0).get("counter_simple")).isEqualTo(42);
        }
    }
}
