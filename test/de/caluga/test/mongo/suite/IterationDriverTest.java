package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 22.03.16
 * Time: 22:37
 * <p>
 * TODO: Add documentation here
 */
public class IterationDriverTest extends MongoTest {

    @Test
    public void iterationTest() throws Exception {
        createUncachedObjects(1999);
        Map<String, Integer> sort = new HashMap<>();
        sort.put("counter", 1);
        MorphiumCursor crs = morphium.getDriver().initIteration(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), new HashMap<>(), sort, null, 0, 5000, 1000, ReadPreference.nearest(), null);

        log.info("got first batch: " + crs.getResult().size());
        assert (crs.getResult().size() == 1000);
        assert (((Map<String, Object>) crs.getResult().get(0)).get("counter").equals(1));
        crs = morphium.getDriver().nextIteration(crs);
        log.info("got next batch:  " + crs.getResult().size());
        assert (crs.getResult().size() == 999);
        assert (((Map<String, Object>) crs.getResult().get(0)).get("counter").equals(1001));
        crs = morphium.getDriver().nextIteration(crs);
        assert (crs == null) : "Cursor should be null!";
    }
}
