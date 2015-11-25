package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.UncachedObject;
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

        List lst = morphium.createQueryFor(UncachedObject.class).distinct("counter");
        assert (lst.size() == 100);
        lst = morphium.createQueryFor(UncachedObject.class).distinct("value");
        assert (lst.size() == 1);
    }
}
