package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.ProfilingListener;
import de.caluga.morphium.ReadAccessType;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 01.08.12
 * Time: 11:11
 * <p/>
 */
public class ProfilingTester extends MorphiumTestBase {
    private boolean readAccess = false, writeAccess = false;
    private long readTime = -1, writeTime = -1;

    @Test
    public void profilingTest() throws Exception {

        ProfilingListener pl = new ProfilingListener() {
            @Override
            public void readAccess(Query query, long time, ReadAccessType t) {
                if (query.getServer() == null) {
                    log.info("did not get Server in query...");
                } else {
                    log.info("Read from Server: " + query.getServer());
                }
                readAccess = true;
                readTime = time;
            }

            @Override
            public void writeAccess(Class type, Object o, long time, boolean isNew, WriteAccessType t) {
                writeAccess = true;
                writeTime = time;
            }
        };

        morphium.addProfilingListener(pl);
        try {
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("Test");
            uc.setCounter(111);
            morphium.store(uc);
            Thread.sleep(100);
            assert (writeAccess);
            assert (writeTime > -1);
            for (int i = 0; i < 100; i++) {
                morphium.createQueryFor(UncachedObject.class).get();
                long s = System.currentTimeMillis();
                while (!readAccess) {
                    Thread.sleep(10);
                    assert (System.currentTimeMillis() - s < 15000);
                }
                assert (readAccess);
                assert (readTime > -1);
            }
        } finally {
            morphium.removeProfilingListener(pl);
        }
    }
}
