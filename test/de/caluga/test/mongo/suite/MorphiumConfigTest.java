package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.Properties;

/**
 * User: Stephan Bösebeck
 * Date: 30.07.13
 * Time: 14:31
 * <p/>
 * TODO: Add documentation here
 */
public class MorphiumConfigTest extends MongoTest {
    @Test
    public void testToString() throws Exception {
        String cfg = MorphiumSingleton.get().getConfig().toString();
        log.info("Config: " + cfg);

        MorphiumConfig c = MorphiumConfig.createFromJson(cfg);
        assert (c.getAdr().size() == 3);
    }

    @Test
    public void testToProperties() throws Exception {
        Properties p = MorphiumSingleton.get().getConfig().asProperties();
        for (Object k : p.keySet()) {
            log.info("KEy: " + k + " Value: " + p.get(k));
        }
        p.store(System.out, "testproperties");

        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assert (cfg.getDatabase().equals(MorphiumSingleton.get().getConfig().getDatabase()));
        assert (cfg.getAdr().size() == 3);
        assert (cfg.getQueryClass() != null);
    }
}
