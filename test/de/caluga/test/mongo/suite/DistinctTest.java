package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 29.04.14
 * Time: 13:24
 * To change this template use File | Settings | File Templates.
 */
public class DistinctTest extends MongoTest {

    @Test
    public void distinctTest() throws Exception {
        createUncachedObjects(100);

        List lst = MorphiumSingleton.get().createQueryFor(UncachedObject.class).distinct("counter");
        assert (lst.size() == 100);
        lst = MorphiumSingleton.get().createQueryFor(UncachedObject.class).distinct("value");
        assert (lst.size() == 1);
    }
}
