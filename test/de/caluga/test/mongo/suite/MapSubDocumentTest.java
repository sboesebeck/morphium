package de.caluga.test.mongo.suite;


import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MapSubDocumentTest extends MorphiumTestBase {

    @Test
    public void testMapSubDocument() throws Exception {
        MapDoc m = new MapDoc();
        m.mapValue = new HashMap<>();
        m.mapValue.put(42L, "life and universe and everything");
        m.mapValue.put(54322321L, "test 2");
        m.value = "Val";

        morphium.store(m);

        MapDoc d = morphium.findById(MapDoc.class, m.id);
        assert (d.value.equals("Val"));
        assert (d.mapValue != null);
        assert (d.mapValue.get(42L).equals("life and universe and everything"));
        assert (d.mapValue.get(54322321L).equals("test 2"));


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
