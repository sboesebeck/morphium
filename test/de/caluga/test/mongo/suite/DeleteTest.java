package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 03.09.12
 * Time: 08:18
 * <p/>
 */
public class DeleteTest extends MongoTest {

    @Test
    public void uncachedDeleteSingle() throws Exception {
        createUncachedObjects(10);
        long c = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (c == 10);
        UncachedObject u = morphium.createQueryFor(UncachedObject.class).get();
        morphium.delete(u);
        c = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (c == 9);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).asList();
        for (UncachedObject uc : lst) {
            assert (!uc.getMorphiumId().equals(u.getMorphiumId()));
        }
    }

    @Test
    public void uncachedDeleteQuery() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(400);
        long c = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (c == 10);
        UncachedObject u = morphium.createQueryFor(UncachedObject.class).get();
        morphium.delete(morphium.createQueryFor(UncachedObject.class).f("counter").eq(u.getCounter()));
        Thread.sleep(400);
        c = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (c == 9);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).asList();
        for (UncachedObject uc : lst) {
            assert (!uc.getMorphiumId().equals(u.getMorphiumId()));
        }
    }

    @Test
    public void cachedDeleteSingle() throws Exception {
        createCachedObjects(10);
        waitForWrites();
        long c = morphium.createQueryFor(CachedObject.class).countAll();
        assert (c == 10) : "Count is " + c;
        CachedObject u = morphium.createQueryFor(CachedObject.class).get();
        morphium.delete(u);
        waitForWrites();

        while (morphium.getStatistics().get("X-Entries for: de.caluga.test.mongo.suite.data.CachedObject").intValue() != 0) {
            log.info("Waiting for cache to be cleared");
            Thread.sleep(250);
        }

        c = morphium.createQueryFor(CachedObject.class).countAll();
        assert (c == 9);
        List<CachedObject> lst = morphium.createQueryFor(CachedObject.class).asList();
        for (CachedObject uc : lst) {
            assert (!uc.getId().equals(u.getId()));
        }
    }

    @Test
    public void cachedDeleteQuery() throws Exception {
        createCachedObjects(10);
        waitForWrites();
        long cnt = morphium.createQueryFor(CachedObject.class).countAll();
        assert (cnt == 10) : "Count is " + cnt;
        CachedObject co = morphium.createQueryFor(CachedObject.class).get();
        morphium.delete(morphium.createQueryFor(CachedObject.class).f("counter").eq(co.getCounter()));
        waitForWrites();
        Thread.sleep(1000);
        cnt = morphium.createQueryFor(CachedObject.class).countAll();
        assert (cnt == 9);
        List<CachedObject> lst = morphium.createQueryFor(CachedObject.class).asList();
        for (CachedObject c : lst) {
            assert (!c.getId().equals(co.getId()));
        }
    }


}
