package de.caluga.test.mongo.suite;

import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 07.05.12
 * Time: 18:02
 * <p/>
 */
public class AliasesTest extends MongoTest {
    @Test
    public void aliasTest() throws Exception {
        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class).f("last_changed").eq(new Date());
        assert (q != null) : "Null Query?!?!?";
        assert (q.toQueryObject().toString().startsWith("{changed=")) : "Wrong query: " + q.toQueryObject().toString();
        log.info("All ok");
    }
}
