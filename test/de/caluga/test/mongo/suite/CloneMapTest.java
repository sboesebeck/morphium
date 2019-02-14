package de.caluga.test.mongo.suite;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 21.04.15
 * Time: 22:00
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings("unchecked")
public class CloneMapTest {
    private Logger log = LoggerFactory.getLogger(CloneMapTest.class);

    @Test
    public void testCompare() {
        Map<String, Object> master = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            master.put("Key " + i, (double) i);
        }
        log.info("starting test using constructor");
        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            HashMap<String, Object> m = new HashMap<>(master);
            m.put("Test " + i, new Object());
            master = m;
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Took " + dur + " ms");

        master = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            master.put("Key " + i, (double) i);
        }
        log.info("starting test using manual copy");
        start = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            HashMap<String, Object> m = new HashMap<>();
            for (Map.Entry<String, Object> e : master.entrySet()) {
                m.put(e.getKey(), e.getValue());
            }
            m.put("Test " + i, new Object());
            master = m;
        }
        dur = System.currentTimeMillis() - start;
        log.info("Took " + dur + " ms");

        master = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            master.put("Key " + i, (double) i);
        }
        log.info("starting test using clone");
        start = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            Map<String, Object> m = (Map<String, Object>) ((HashMap) master).clone();
            m.put("Test " + i, new Object());
            master = m;
        }
        dur = System.currentTimeMillis() - start;
        log.info("Took " + dur + " ms");
    }
}
