package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.Utils;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.11.15
 * Time: 22:47
 * <p>
 * TODO: Add documentation here
 */
public class QueryTest extends MongoTest {
    @Test
    public void testToQueryObject() throws Exception {
        Query q = morphium.createQueryFor(UncachedObject.class);
        q.setType(UncachedObject.class);
        q.f("counter").eq(123);
        System.out.println(Utils.toJsonString(q.toQueryObject()));

        q = q.q().f("counter").lt(100).f("counter").gt(10);
        System.out.println(Utils.toJsonString(q.toQueryObject()));
    }
}
