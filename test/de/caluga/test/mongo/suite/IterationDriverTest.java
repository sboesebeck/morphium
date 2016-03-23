package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.ReadPreference;
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
    public static Morphium[] morphiums = getMorphiums().toArray(new Morphium[getMorphiums().size()]);


    @Theory
    public void iterationTest(Morphium morphium) throws Exception {
        logSeparator("Using Driver " + morphium.getDriver().getClass().getName());

        createUncachedObjects(morphium, 1999);

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

        //cleaning up
        morphium.dropCollection(UncachedObject.class);
    }


}
