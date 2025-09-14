package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.test.mongo.suite.data.PlainContainer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Tag("core")
public class PlainConatinerTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void plainListTest(Morphium morphium) throws Exception {
        try (morphium) {
            PlainContainer pc = new PlainContainer();
            pc.setPlainList(Arrays.asList("str1", "str2"));
            morphium.store(pc);
            Thread.sleep(200);
            PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
            assertEquals(pc.getId(), pc2.getId());
            assertEquals(2, pc2.getPlainList().size());
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
            //sleep to be sure all slaves are updated
            Thread.sleep(100);
            PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
            assertEquals(pc.getId(), pc2.getId());
            assertEquals(2, pc2.getPlainMap().size());
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
            Thread.sleep(200);
            PlainContainer pc2 = morphium.findById(PlainContainer.class, pc.getId());
            assertEquals(pc.getId(), pc2.getId());
            assertEquals(2, pc2.getPlainMap().size());
            assertTrue(pc2.getPlainMap().get("test2") instanceof List);
            assertEquals(2, ((List) pc2.getPlainMap().get("test2")).size());
            assertEquals("str1", ((List) pc2.getPlainMap().get("test2")).get(0));
        }
    }
}
