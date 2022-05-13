package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.test.mongo.suite.data.PlainContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class PlainConatinerTest extends MorphiumTestBase {

    @Test
    public void plainListTest() throws Exception {
        PlainContainer pc = new PlainContainer();
        pc.setPlainList(Arrays.asList("str1", "str2"));

        morphium.store(pc);

        PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
        assertThat(pc2.getId()).isEqualTo(pc.getId());
        assertThat(pc2.getPlainList().size()).isEqualTo(2);
    }

    @Test
    public void plainMapTest() throws Exception {
        PlainContainer pc = new PlainContainer();
        //pc.setPlainMap(Map.of("test",(Object)"value").add("test2",Arrays.asList("str1","stre2")));
        pc.setPlainMap(Map.of("$in", (Object) "value", "$test2", Map.of("$str1", "stre2")));

        morphium.store(pc);

        PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
        assertThat(pc2.getId()).isEqualTo(pc.getId());
        assertThat(pc2.getPlainMap().size()).isEqualTo(2);
    }

    @Test
    public void plainMapListTest() throws Exception {
        PlainContainer pc = new PlainContainer();
        pc.setPlainMap(Map.of("test", (Object) "value", "test2", Arrays.asList("str1", "stre2")));
        //pc.setPlainMap(Map.of("$in",(Object)"value").add("$test2",Map.of("$str1","stre2")));

        morphium.store(pc);

        PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
        assertThat(pc2.getId()).isEqualTo(pc.getId());
        assertThat(pc2.getPlainMap().size()).isEqualTo(2);
        assertThat(pc2.getPlainMap().get("test2")).isInstanceOf(List.class);
        assertThat(((List) pc2.getPlainMap().get("test2")).size()).isEqualTo(2);
        assertThat(((List) pc2.getPlainMap().get("test2")).get(0)).isEqualTo("str1");
    }
}