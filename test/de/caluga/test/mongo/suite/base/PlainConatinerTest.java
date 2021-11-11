package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.test.mongo.suite.data.PlainContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Arrays;

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
        //pc.setPlainMap(Utils.getMap("test",(Object)"value").add("test2",Arrays.asList("str1","stre2")));
        pc.setPlainMap(Utils.getMap("$in", (Object) "value").add("$test2", Utils.getMap("$str1", "stre2")));

        morphium.store(pc);

        PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
        assertThat(pc2.getId()).isEqualTo(pc.getId());
        assertThat(pc2.getPlainMap().size()).isEqualTo(2);
    }

    @Test
    public void plainMapListTest() throws Exception {
        PlainContainer pc = new PlainContainer();
        pc.setPlainMap(Utils.getMap("test", (Object) "value").add("test2", Arrays.asList("str1", "stre2")));
        //pc.setPlainMap(Utils.getMap("$in",(Object)"value").add("$test2",Utils.getMap("$str1","stre2")));

        morphium.store(pc);

        PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
        assertThat(pc2.getId()).isEqualTo(pc.getId());
        assertThat(pc2.getPlainMap().size()).isEqualTo(2);
    }
}
