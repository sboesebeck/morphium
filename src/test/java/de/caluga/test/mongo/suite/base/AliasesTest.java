package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.AliasesEntity;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * User: Stephan Bösebeck
 * Date: 07.05.12
 * Time: 18:02
 * <p/>
 */
@Tag("core")
public class AliasesTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aliasTest(Morphium morphium) throws Exception {
        try (morphium) {
            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class).f("last_changed").eq(new Date());
            assertNotNull(q, "Null Query?!?!?");
            assert(q.toQueryObject().toString().startsWith("{changed=")) : "Wrong query: " + q.toQueryObject().toString();
            log.info("All ok");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aliasReadTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            Map<String, Object> stats = morphium.storeMap(ComplexObject.class, UtilsMap.of("last_changed", (Object) System.currentTimeMillis(),
                                                "_id", id,
                                                "einText", "A little text")
                                                         );
            assertNotNull(stats);
            assertEquals(1, stats.get("n"));
            Thread.sleep(100);
            List<ComplexObject> lst = morphium.createQueryFor(ComplexObject.class).asList();
            assertEquals(1, lst.size());
            assertEquals(id, lst.get(0).getId());
            assertEquals("A little text", lst.get(0).getEinText());
            assertThat(lst.get(0).getChanged()).isNotEqualTo(0);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aliasesPreferenceTests(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            AliasesEntity ae = new AliasesEntity();
            ae.setValue("a value");
            ae.setValues(Arrays.asList("value1", "v2"));
            Map<String, Object> m = morphium.getMapper().serialize(ae);
            m.put("the_value", "other value");
            m.put("_id", id);
            morphium.storeMap(AliasesEntity.class, m);
            Thread.sleep(150);
            AliasesEntity ae2 = morphium.createQueryFor(AliasesEntity.class).f("_id").eq(id).get();
            assertEquals("a value", ae2.getValue()); //if primary name is present, do not use aliases
            assertNotNull(ae2.getValues());
            assertThat(ae2.getValues()).isNotEmpty();
            assertEquals(2, ae2.getValues().size());
            assertEquals("value1", ae2.getValues().get(0));
            assertEquals("v2", ae2.getValues().get(1));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aliasesListTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            AliasesEntity ae = new AliasesEntity();
            ae.setValue("a value");
            ae.setValues(Arrays.asList("value1", "v2"));
            Map<String, Object> m = morphium.getMapper().serialize(ae);
            m.put("the_value", m.get("value"));
            m.remove("value");
            m.put("_id", id);
            m.put("lots_of_values", m.get("values"));
            m.remove("values");
            morphium.storeMap(AliasesEntity.class, m);
            Thread.sleep(150);
            AliasesEntity ae2 = morphium.createQueryFor(AliasesEntity.class).f("_id").eq(id).get();
            assertEquals("a value", ae2.getValue()); //if primary name is present, do not use aliases
            assertNotNull(ae2.getValues());
            assertThat(ae2.getValues()).isNotEmpty();
            assertEquals(2, ae2.getValues().size());
            assertEquals("value1", ae2.getValues().get(0));
            assertEquals("v2", ae2.getValues().get(1));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aliasesRefListTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            AliasesEntity ae = new AliasesEntity();
            ae.setValue("a value");
            ae.setReferences(Arrays.asList(new UncachedObject("str1", 1), new UncachedObject("str2", 2)));
            Map<String, Object> m = morphium.getMapper().serialize(ae);
            m.put("the_value", m.get("value"));
            m.remove("value");
            m.put("_id", id);
            m.put("lots_of_values", m.get("values"));
            m.remove("values");
            morphium.storeMap(AliasesEntity.class, m);
            Thread.sleep(150);
            AliasesEntity ae2 = morphium.createQueryFor(AliasesEntity.class).f("_id").eq(id).get();
            assertEquals("a value", ae2.getValue()); //if primary name is present, do not use aliases
            assertNotNull(ae2.getReferences());
            assertThat(ae2.getReferences()).isNotEmpty();
            assertEquals(2, ae2.getReferences().size());
            assertThat(ae2.getReferences().get(0)).isInstanceOf(UncachedObject.class);
            assertThat(ae2.getReferences().get(1)).isInstanceOf(UncachedObject.class);
            assertEquals("str2", ((UncachedObject) ae2.getReferences().get(1)).getStrValue());
        }
    }
}
