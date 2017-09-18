package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Created by stephan on 29.07.16.
 */
@RunWith(Theories.class)
public class TailableQueryTests extends MongoTest {
    @DataPoints
    public static final Morphium[] morphiums = new Morphium[]{morphiumMeta, morphiumMongodb};//getMorphiums().toArray(new Morphium[getMorphiums().size()]);
    boolean found = false;

    @Theory
    public void tailableTest(Morphium m) throws Exception {
        m.dropCollection(CappedCollectionTest.CappedCol.class);
        CappedCollectionTest.CappedCol o = new CappedCollectionTest.CappedCol("Test1", 1);
        m.store(o);
        m.store(new CappedCollectionTest.CappedCol("Test 2", 2));
        found = false;
        new Thread() {
            public void run() {
                Query<CappedCollectionTest.CappedCol> q = m.createQueryFor(CappedCollectionTest.CappedCol.class);
                q.tail(10, 0, new AsyncCallbackAdapter<CappedCollectionTest.CappedCol>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<CappedCollectionTest.CappedCol> q, long duration, List<CappedCollectionTest.CappedCol> result, CappedCollectionTest.CappedCol entity, Object... param) {
                        log.info("Got incoming!!! " + entity.getValue() + " " + entity.getCounter());
                        found = true;
                        if (entity.getValue().equals("Test 3 - quit")) {
                            throw new MorphiumAccessVetoException("Quitting");
                        }
                    }
                });
                assert (found);
            }
        }.start();

        Thread.sleep(1500);
        assert (found);
        found = false;
        log.info("Storing 3...");
        m.store(new CappedCollectionTest.CappedCol("Test 3 - quit", 3));
        Thread.sleep(1500);
        assert (found);

    }
}
