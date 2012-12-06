package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.ProfilingListener;
import de.caluga.morphium.ReadAccessType;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 01.08.12
 * Time: 11:11
 * <p/>
 */
public class ProfilingTester extends MongoTest {
    private boolean readAccess = false, writeAccess = false;
    private long readTime = -1, writeTime = -1;

    @Test
    public void profilingTest() throws Exception {

        ProfilingListener pl = new ProfilingListener() {
            @Override
            public void readAccess(Query query, long time, ReadAccessType t) {
                readAccess = true;
                readTime = time;
            }

            @Override
            public void writeAccess(Class type, Object o, long time, boolean isNew, WriteAccessType t) {
                writeAccess = true;
                writeTime = time;
            }
        };

        MorphiumSingleton.get().addProfilingListener(pl);

        UncachedObject uc = new UncachedObject();
        uc.setValue("Test");
        uc.setCounter(111);
        MorphiumSingleton.get().store(uc);
        assert (writeAccess);
        assert (writeTime > -1);

        MorphiumSingleton.get().createQueryFor(UncachedObject.class).get();
        assert (readAccess);
        assert (readTime > -1);
        MorphiumSingleton.get().removeProfilingListener(pl);
    }
}
