package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.MorphiumId;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Vector;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:29
 * <p>
 * TODO: Add documentation here
 */
@Tag("core")
public class MorphiumIdTest {

    @Test
    public void threaddedCreationTest() {
        for (int i = 0; i < 1000; i++) {
            new Thread(MorphiumId::new).start();
        }
    }

    @Test
    public void testCollision() throws Exception {
        final List<MorphiumId> ids = new Vector<>();
        for (int i = 0; i < 3000; i++) {
            MorphiumId id = new MorphiumId();
            assertFalse(ids.contains(id));
            ids.add(id);
        }

        List<Thread> thr = new Vector<>();
        for (int i = 0; i < 100; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 200; j++) {
                        MorphiumId id = new MorphiumId();
                        MorphiumId id2 = new MorphiumId();
                        MorphiumId id3 = new MorphiumId();
                        assertFalse(ids.contains(id));
                        assertFalse(ids.contains(id2));
                        assertFalse(ids.contains(id3));
                        ids.add(id);
                        ids.add(id2);
                        ids.add(id3);
                    }
                    thr.remove(this);
                }
            };
            thr.add(t);
            t.start();
        }
        while (thr.size() > 0) {
            LoggerFactory.getLogger(MorphiumIdTest.class).info("Waiting for threads..." + thr.size());
            Thread.sleep(1000);
        }
        assertEquals(100 * 200 * 3 + 3000, ids.size(), "Amount ids created");
    }
}
