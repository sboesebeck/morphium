package de.caluga.test.mongo.suite;/**
 * Created by stephan on 14.07.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

/**
 * TODO: Add Documentation here
 **/
public class ConstructorTest {

    @Test
    public void testConstructors() throws Exception {
        Morphium m = new Morphium("localhost", "morphium-test");
        m.createQueryFor(UncachedObject.class).limit(10).asList();
        m.close();

        m = new Morphium("localhost", 27017, "morphium-test");
        m.createQueryFor(UncachedObject.class).limit(10).asList();
        m.close();

        MorphiumConfig cfg = new MorphiumConfig("morphium-test", 10, 1000, 1000);
        cfg.addHostToSeed("localhost");
        m = new Morphium(cfg);
        m.createQueryFor(UncachedObject.class).limit(10).asList();
        m.close();

        cfg = new MorphiumConfig("morphium-test", 10, 1000, 1000);
        cfg.addHostToSeed("localhost");
        m = new Morphium();
        m.setConfig(cfg);
        m.createQueryFor(UncachedObject.class).limit(10).asList();
        m.close();
    }

    @Test
    public void severalMorphiumTest() throws Exception {
        Morphium m = new Morphium("localhost", "morphium-test");
        UncachedObject o = new UncachedObject();
        o.setValue("Tst: " + System.nanoTime());
        o.setCounter((int) System.currentTimeMillis());
        m.store(o);

        assert (m.createQueryFor(UncachedObject.class).countAll() >= 1);

        MorphiumConfig cfg = new MorphiumConfig("morphium-test", 10, 1000, 1000);
        cfg.addHostToSeed("localhost");
        Morphium m2 = new Morphium(cfg);

        assert (m2.createQueryFor(UncachedObject.class).countAll() >= 1);

        assert (m2.findById(UncachedObject.class, o.getMorphiumId()) != null);

        m2.close();
        assert (m.createQueryFor(UncachedObject.class).countAll() >= 1);
    }
}
