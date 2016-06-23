package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 22.03.16
 * Time: 22:37
 * <p>
 * TODO: Add documentation here
 */

@RunWith(Theories.class)
public class IterationDriverTest extends MongoTest {
    @DataPoints
    public static final Morphium[] morphiums = new Morphium[]{morphiumInMemeory, morphiumSingleConnect, morphiumSingleConnectThreadded, morphiumMeta, morphiumMongodb};//getMorphiums().toArray(new Morphium[getMorphiums().size()]);


    @Theory
    public void iterationTest(Morphium morphium) throws Exception {
        logSeparator("Using Driver " + morphium.getDriver().getClass().getName());

        createUncachedObjects(morphium, 1999);
        Thread.sleep(250);
        Map<String, Integer> sort = new HashMap<>();
        sort.put("counter", 1);
        MorphiumCursor crs = morphium.getDriver().initIteration(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), new HashMap<>(), sort, null, 0, 5000, 1000, ReadPreference.nearest(), null);
        assert (crs != null);
        log.info("got first batch: " + crs.getBatch().size());
        assert (crs.getBatch().size() == 1000);
        assert (((Map<String, Object>) crs.getBatch().get(0)).get("counter").equals(1));
        crs = morphium.getDriver().nextIteration(crs);
        log.info("got next batch:  " + crs.getBatch().size());
        assert (crs.getBatch().size() == 999);
        assert (((Map<String, Object>) crs.getBatch().get(0)).get("counter").equals(1001));
        crs = morphium.getDriver().nextIteration(crs);
        assert (crs == null) : "Cursor should be null!";
        morphium.getDriver().closeIteration(null);

        //cleaning up
        morphium.dropCollection(UncachedObject.class);
        Thread.sleep(250);
    }


    @Theory
    public void closeIterationTest(Morphium morphium) throws Exception {
        logSeparator("Using Driver " + morphium.getDriver().getClass().getName());

        createUncachedObjects(morphium, 1999);
        Thread.sleep(250);
        Map<String, Integer> sort = new HashMap<>();
        sort.put("counter", 1);
        MorphiumCursor crs = morphium.getDriver().initIteration(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), new HashMap<>(), sort, null, 0, 5000, 1000, ReadPreference.nearest(), null);
        assert (crs != null);
        log.info("got first batch: " + crs.getBatch().size());
        assert (crs.getBatch().size() == 1000);
        assert (((Map<String, Object>) crs.getBatch().get(0)).get("counter").equals(1));

        morphium.getDriver().closeIteration(crs);
        try {
            crs = morphium.getDriver().nextIteration(crs);
            log.error("No exception when getting next iteration - ERROR!");
        } catch (MorphiumDriverException e) {
            log.info("Expected exception...", e);
        }


        //cleaning up
        morphium.dropCollection(UncachedObject.class);
        Thread.sleep(1250);
    }


    @Theory
    public void iterableTest(Morphium morphium) throws Exception {
        logSeparator("Using Driver " + morphium.getDriver().getClass().getName());
        int amount = 19999;
        createUncachedObjects(morphium, amount);
        Thread.sleep(250);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        int count = 0;
        long start = System.currentTimeMillis();
        for (UncachedObject o : q.asIterable()) {
            count++;
        }
        long dur = System.currentTimeMillis() - start;
        assert (count == amount) : "Wront amount got " + count + " instead of " + amount;
        log.info("Duration with batchsize and morphiumdriveriterator took " + dur + "ms");

        count = 0;
        start = System.currentTimeMillis();
        for (UncachedObject o : q.asIterable(morphium.getConfig().getCursorBatchSize())) {
            count++;
        }
        dur = System.currentTimeMillis() - start;
        assert (count == amount);
        log.info("Duration with batchsize and DefaultMorphiumIterator took " + dur + "ms");

        count = 0;
        start = System.currentTimeMillis();
        for (UncachedObject o : q.asIterable(100, 4)) {
            count++;
        }
        dur = System.currentTimeMillis() - start;
        assert (count == amount) : "Count mismatch... chould be " + amount + " but is " + count;
        log.info("Duration with batchsize 100/lookahed 4 took " + dur + "ms");

        //cleaning up
        morphium.dropCollection(UncachedObject.class);
        Thread.sleep(250);
    }

}
