package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 12.07.12
 * Time: 16:46
 * <p/>
 */
public class SkipLimitTest extends MongoTest {
    @Test
    public void skipTest() throws Exception {
        createUncachedObjects(100);
        UncachedObject o = MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").lt(100).skip(10).sort("counter").get();
        assert (o.getCounter() == 11);

    }


    @Test
    public void limitTest() throws Exception {
        createUncachedObjects(100);

        List<UncachedObject> l = MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").lt(100).limit(10).sort("counter").asList();
        assert (l.size() == 10);
    }

    @Test
    public void skipLimitTest() throws Exception {
        createUncachedObjects(100);

        List<UncachedObject> l = MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").lt(100).skip(50).limit(10).sort("counter").asList();
        assert (l.size() == 10);
        assert (l.get(0).getCounter() == 51);
    }

}
