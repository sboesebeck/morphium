package de.caluga.test.mongo.suite;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.HashMap;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 13:49
 * <p/>
 * TODO: Add documentation here
 */
public class CollectionNameOverrideTest extends MongoTest {
    @Test
    public void writeCollectionNameOverride() throws Exception {
        morphium.dropCollection(UncachedObject.class, "uncached_collection_test_2", null);
        UncachedObject uc = new UncachedObject();
        uc.setValue("somewhere different");
        uc.setCounter(1000);
        morphium.store(uc, "uncached_collection_test_2", null);
        Thread.sleep(1000);
        //should be in a different colleciton now
        //        c = morphium.getDatabase().getCollection("uncached_collection_test_2");

        long count = morphium.getDriver().count(morphium.getConfig().getDatabase(), "uncached_collection_test_2", new HashMap<>(), null);
        assert (count == 1) : "Count is: " + count;
    }


    @Test
    public void writeAndReadCollectionNameOverride() throws Exception {
        morphium.dropCollection(UncachedObject.class, "uncached_collection_test_2", null);
        UncachedObject uc = new UncachedObject();
        uc.setValue("somewhere different");
        uc.setCounter(1000);
        morphium.store(uc, "uncached_collection_test_2", null);
        Thread.sleep(1000);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.countAll() == 0);
        q.setCollectionName("uncached_collection_test_2");
        assert (q.countAll() == 1);

    }

    @Test
    public void cacheTest() throws Exception {
        morphium.dropCollection(UncachedObject.class, "uncached_collection_test_2", null);
        createCachedObjects(100);
        CachedObject o = new CachedObject();
        o.setCounter(20000);
        o.setValue("different collection");
        morphium.store(o, "cached_collection_test_2", null);

    }
}
