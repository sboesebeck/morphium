package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 29.11.15
 * Time: 23:46
 * <p>
 * TODO: Add documentation here
 */
public class InMemoryTest {

    @Test
    public void inMemoryDriverTest() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();

        List<Map<String, Object>> toStore = new ArrayList<>();
        Map<String, Object> o = Utils.getMap("key", "a_value");
        o.put("key2", 123);
        toStore.add(o);

        drv.store("test", "test_coll", toStore, null);

        assert (o.get("_id") != null);


        toStore = new ArrayList<>();
        o = Utils.getMap("key", "another value");
        o.put("key2", 125);
        toStore.add(o);

        drv.store("test", "test_coll", toStore, null);

        assert (o.get("_id") != null);

        List<Map<String, Object>> lst = drv.find("test", "test_coll", Utils.getMap("key", "a_value"), null, null, 0, 0, 0, null, null);
        assert (lst.size() == 1);
    }

    @Test
    public void morphiumInMemoryTest() throws Exception {
        MorphiumConfig c = new MorphiumConfig("mem_test", 1000, 1000, 10000);
        c.addHostToSeed("localhost");
        c.setDriverClass(InMemoryDriver.class.getName());
        Morphium m = new Morphium(c);
        UncachedObject uc = new UncachedObject();
        uc.setCounter(1213);
        m.store(uc);

        UncachedObject u = m.reread(uc);
        assert (u != null);
        assert (uc.getMorphiumId() != null);
        assert (u.getMorphiumId() != null);
    }
}
