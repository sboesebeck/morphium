package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.MorphiumId;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:29
 * <p>
 * TODO: Add documentation here
 */
public class MorphiumIdTest {

    @Test
    public void threaddedCreationTest() {
        for (int i = 0; i < 1000; i++) {
            new Thread() {
                @Override
                public void run() {
                    new MorphiumId();
                }
            }.start();
        }
    }

    @Test
    public void testCollision() throws Exception {
        final List<MorphiumId> ids = new Vector<>();
        for (int i = 0; i < 1000; i++) {
            MorphiumId id = new MorphiumId();
            assert (!ids.contains(id));
            ids.add(id);
        }

        ids.clear();
        List<Thread> thr = new Vector<>();
        for (int i = 0; i < 200; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 250; j++) {
                        MorphiumId id = new MorphiumId();
                        assert (!ids.contains(id));
                        ids.add(id);
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
    }
}
