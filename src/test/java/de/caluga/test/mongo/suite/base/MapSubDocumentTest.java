package de.caluga.test.mongo.suite.base;


import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MapSubDocumentTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testMapSubDocument(Morphium morphium) throws Exception {
        try (morphium) {
            MapDoc m = new MapDoc();
            m.mapValue = new HashMap<>();
            m.mapValue.put(42L, "life and universe and everything");
            m.mapValue.put(54322321L, "test 2");
            m.value = "Val";
            morphium.store(m);
            Thread.sleep(500);
            MapDoc d = morphium.findById(MapDoc.class, m.id);
            assert(d.value.equals("Val"));
            assertNotNull(d.mapValue);
            ;
            assert(d.mapValue.get(42L).equals("life and universe and everything"));
            assert(d.mapValue.get(54322321L).equals("test 2"));
        }

        //this test will fail with map keys that cannot easily be translated
        //from and to string. E.g. a key like 123.45 will NOT work!
        //TODO: handle storage of SubDocuments with non-String keys
    }


    @Entity
    public static class MapDoc {
        @Id
        public MorphiumId id;
        public String value;
        public Map<Long, String> mapValue;
    }
}
