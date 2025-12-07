package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 13:49
 * <p>
 * TODO: Add documentation here
 */
@Tag("core")
public class CollectionNameOverrideTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void writeCollectionNameOverride(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class, "uncached_collection_test_2", null);
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("somewhere different");
            uc.setCounter(1000);
            morphium.store(uc, "uncached_collection_test_2", null);
            Thread.sleep(1000);
            //should be in a different colleciton now
            //        c = morphium.getDatabase().getCollection("uncached_collection_test_2");

            //long count = morphium.getDriver().count(morphium.getConfig().getDatabase(), "uncached_collection_test_2", new HashMap<>(), null, null);
            // assert (count == 1) : "Count is: " + count;
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void writeAndReadCollectionNameOverride(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class, "uncached_collection_test_2", null);
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("somewhere different");
            uc.setCounter(1000);
            morphium.store(uc, "uncached_collection_test_2", null);
            Thread.sleep(1000);

            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            assert (q.countAll() == 0);
            q.setCollectionName("uncached_collection_test_2");
            assert (q.countAll() == 1);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cacheTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class, "uncached_collection_test_2", null);
            createCachedObjects(morphium, 100);
            CachedObject o = new CachedObject();
            o.setCounter(20000);
            o.setValue("different collection");
            morphium.store(o, "cached_collection_test_2", null);
        }
    }
}
