package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.test.mongo.suite.data.PlainContainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PlainConatinerTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void plainListTest(Morphium morphium) throws Exception {
        try (morphium) {
            PlainContainer pc = new PlainContainer();
            pc.setPlainList(Arrays.asList("str1", "str2"));

            morphium.store(pc);

            PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
            assertThat(pc2.getId()).isEqualTo(pc.getId());
            assertThat(pc2.getPlainList().size()).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void plainMapTest(Morphium morphium) throws Exception {
        try (morphium) {
            PlainContainer pc = new PlainContainer();
            //pc.setPlainMap(UtilsMap.of("test",(Object)"value").add("test2",Arrays.asList("str1","stre2")));
            pc.setPlainMap(UtilsMap.of("$in", (Object) "value", "$test2", UtilsMap.of("$str1", "stre2")));

            morphium.store(pc);

            PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
            assertThat(pc2.getId()).isEqualTo(pc.getId());
            assertThat(pc2.getPlainMap().size()).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void plainMapListTest(Morphium morphium) throws Exception {
        try (morphium) {
            PlainContainer pc = new PlainContainer();
            pc.setPlainMap(UtilsMap.of("test", (Object) "value", "test2", Arrays.asList("str1", "stre2")));
            //pc.setPlainMap(UtilsMap.of("$in",(Object)"value").add("$test2",UtilsMap.of("$str1","stre2")));

            morphium.store(pc);

            PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
            assertThat(pc2.getId()).isEqualTo(pc.getId());
            assertThat(pc2.getPlainMap().size()).isEqualTo(2);
            assertThat(pc2.getPlainMap().get("test2")).isInstanceOf(List.class);
            assertThat(((List) pc2.getPlainMap().get("test2")).size()).isEqualTo(2);
            assertThat(((List) pc2.getPlainMap().get("test2")).get(0)).isEqualTo("str1");
        }
    }
}
