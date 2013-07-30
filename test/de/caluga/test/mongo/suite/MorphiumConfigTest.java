package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

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
}
