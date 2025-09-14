package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 19.04.13
 * Time: 12:15
 * <p/>
 * TODO: Add documentation here
 */
@Tag("core")
public class IDConversionTest extends MorphiumTestBase {
    @Test
    public void testIdConversion() {
        Query qu = new Query(morphium, UncachedObject.class, null);
        qu.setCollectionName("uncached");
        qu.f("_id").eq(new MorphiumId().toString());

        System.out.println(qu.toQueryObject().toString());
        assert (qu.toQueryObject().toString().contains("_id="));

        qu = new Query(morphium, UncachedObject.class, null);
        qu.setCollectionName("uncached");
        qu.f("str_value").eq(new MorphiumId());
        System.out.println(qu.toQueryObject().toString());
        assert (!qu.toQueryObject().toString().contains("_id="));
    }
}
