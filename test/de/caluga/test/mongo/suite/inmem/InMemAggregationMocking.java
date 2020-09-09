package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemAggregationMocking extends MorphiumInMemTestBase {


    @Test
    public void mockAggregation() throws Exception {
        MorphiumDriver original = morphium.getDriver();
        morphium.setDriver(new InMemoryDriver() {
            @Override
            public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, Collation collation, ReadPreference readPreference) throws MorphiumDriverException {
                return Arrays.asList(Utils.getMap("MockedData", 123.0d));
            }
        });

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        assert (agg.aggregate().get(0).get("MockedData").equals(123.0d));
        morphium.getDriver().close();
        morphium.setDriver(original);
    }
}
