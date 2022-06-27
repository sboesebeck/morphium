package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.AliasesEntity;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * User: Stephan Bösebeck
 * Date: 07.05.12
 * Time: 18:02
 * <p/>
 */
public class AliasesTest extends MorphiumTestBase {
    @Test
    public void aliasTest() {
        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class).f("last_changed").eq(new Date());
        assert (q != null) : "Null Query?!?!?";
        assert (q.toQueryObject().toString().startsWith("{changed=")) : "Wrong query: " + q.toQueryObject().toString();
        log.info("All ok");
    }

    @Test
    public void aliasReadTest() throws Exception {
        MorphiumId id = new MorphiumId();
        Map<String, Object> stats = morphium.storeMap(ComplexObject.class, UtilsMap.of("last_changed", (Object) System.currentTimeMillis(),
                "_id", id,
                "einText", "A little text")
        );
        assertThat(stats).isNotNull();
        assertThat(stats.get("n")).isEqualTo(1);

        List<ComplexObject> lst = morphium.createQueryFor(ComplexObject.class).asList();
        assertThat(lst.size()).isEqualTo(1);
        assertThat(lst.get(0).getId()).isEqualTo(id);
        assertThat(lst.get(0).getEinText()).isEqualTo("A little text");
        assertThat(lst.get(0).getChanged()).isNotEqualTo(0);
    }


    @Test
    public void aliasesPreferenceTests() throws Exception {
        MorphiumId id = new MorphiumId();
        AliasesEntity ae = new AliasesEntity();
        ae.setValue("a value");
        ae.setValues(Arrays.asList("value1", "v2"));
        Map<String, Object> m = morphium.getMapper().serialize(ae);
        m.put("the_value", "other value");
        m.put("_id", id);
        morphium.storeMap(AliasesEntity.class, m);

        AliasesEntity ae2 = morphium.createQueryFor(AliasesEntity.class).f("_id").eq(id).get();
        assertThat(ae2.getValue()).isEqualTo("a value"); //if primary name is present, do not use aliases
        assertThat(ae2.getValues()).isNotNull();
        assertThat(ae2.getValues()).isNotEmpty();
        assertThat(ae2.getValues().size()).isEqualTo(2);
        assertThat(ae2.getValues().get(0)).isEqualTo("value1");
        assertThat(ae2.getValues().get(1)).isEqualTo("v2");
    }

    @Test
    public void aliasesListTest() throws Exception {
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

        AliasesEntity ae2 = morphium.createQueryFor(AliasesEntity.class).f("_id").eq(id).get();
        assertThat(ae2.getValue()).isEqualTo("a value"); //if primary name is present, do not use aliases
        assertThat(ae2.getValues()).isNotNull();
        assertThat(ae2.getValues()).isNotEmpty();
        assertThat(ae2.getValues().size()).isEqualTo(2);
        assertThat(ae2.getValues().get(0)).isEqualTo("value1");
        assertThat(ae2.getValues().get(1)).isEqualTo("v2");
    }

    @Test
    public void aliasesRefListTest() throws Exception {
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

        AliasesEntity ae2 = morphium.createQueryFor(AliasesEntity.class).f("_id").eq(id).get();
        assertThat(ae2.getValue()).isEqualTo("a value"); //if primary name is present, do not use aliases
        assertThat(ae2.getReferences()).isNotNull();
        assertThat(ae2.getReferences()).isNotEmpty();
        assertThat(ae2.getReferences().size()).isEqualTo(2);
        assertThat(ae2.getReferences().get(0)).isInstanceOf(UncachedObject.class);
        assertThat(ae2.getReferences().get(1)).isInstanceOf(UncachedObject.class);
        assertThat(((UncachedObject) ae2.getReferences().get(1)).getStrValue()).isEqualTo("str2");
    }
}
