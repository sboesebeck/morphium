package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 30.06.12
 * Time: 13:58
 * <p/>
 */
public class IdCacheTest extends MongoTest {

    @Test
    public void idTest() throws Exception {
        for (int i = 1; i < 100; i++) {
            CachedObject u = new CachedObject();
            u.setCounter(i);
            u.setValue("Counter = " + i);
            MorphiumSingleton.get().store(u);
        }

        waitForWrites();
        Thread.sleep(5000);

        Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        q = q.f("counter").lt(30);
        List<CachedObject> lst = q.asList();
        assert (lst.size() == 29) : "Size matters! " + lst.size();

        ObjectId id = lst.get(0).getId();
        CachedObject c = MorphiumSingleton.get().findById(CachedObject.class, id);
        assert (lst.get(0) == c) : "Object differ?";

        c.setCounter(1009);
        assert (lst.get(0).getCounter() == 1009) : "changes not work?";

        MorphiumSingleton.get().reread(c);
        assert (c.getCounter() != 1009) : "reread did not work?";

        assert (lst.get(0) == c) : "Object changed?!?!?";
    }
}
