package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 03.09.12
 * Time: 08:18
 * <p/>
 * TODO: Add documentation here
 */
public class DeleteTest extends MongoTest {

    @Test
    public void uncachedDeleteSingle() throws Exception {
        createUncachedObjects(10);
        long c = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
        assert (c == 10);
        UncachedObject u = MorphiumSingleton.get().createQueryFor(UncachedObject.class).get();
        MorphiumSingleton.get().delete(u);
        c = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
        assert (c == 9);
        List<UncachedObject> lst = MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList();
        for (UncachedObject uc : lst) {
            assert (!uc.getMongoId().equals(u.getMongoId()));
        }
    }

    @Test
    public void uncachedDeleteQuery() throws Exception {
        createUncachedObjects(10);
        long c = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
        assert (c == 10);
        UncachedObject u = MorphiumSingleton.get().createQueryFor(UncachedObject.class).get();
        MorphiumSingleton.get().delete(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").eq(u.getCounter()));
        c = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
        assert (c == 9);
        List<UncachedObject> lst = MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList();
        for (UncachedObject uc : lst) {
            assert (!uc.getMongoId().equals(u.getMongoId()));
        }
    }

    @Test
    public void cachedDeleteSingle() throws Exception {
        createCachedObjects(10);
        waitForWrites();
        long c = MorphiumSingleton.get().createQueryFor(CachedObject.class).countAll();
        assert (c == 10) : "Count is " + c;
        CachedObject u = MorphiumSingleton.get().createQueryFor(CachedObject.class).get();
        MorphiumSingleton.get().delete(u);
        waitForWrites();
        c = MorphiumSingleton.get().createQueryFor(CachedObject.class).countAll();
        assert (c == 9);
        List<CachedObject> lst = MorphiumSingleton.get().createQueryFor(CachedObject.class).asList();
        for (CachedObject uc : lst) {
            assert (!uc.getId().equals(u.getId()));
        }
    }

    @Test
    public void cachedDeleteQuery() throws Exception {
        createCachedObjects(10);
        waitForWrites();
        long cnt = MorphiumSingleton.get().createQueryFor(CachedObject.class).countAll();
        assert (cnt == 10) : "Count is " + cnt;
        CachedObject co = MorphiumSingleton.get().createQueryFor(CachedObject.class).get();
        MorphiumSingleton.get().delete(MorphiumSingleton.get().createQueryFor(CachedObject.class).f("counter").eq(co.getCounter()));
        waitForWrites();
        Thread.sleep(1000);
        cnt = MorphiumSingleton.get().createQueryFor(CachedObject.class).countAll();
        assert (cnt == 9);
        List<CachedObject> lst = MorphiumSingleton.get().createQueryFor(CachedObject.class).asList();
        for (CachedObject c : lst) {
            assert (!c.getId().equals(co.getId()));
        }
    }


}
