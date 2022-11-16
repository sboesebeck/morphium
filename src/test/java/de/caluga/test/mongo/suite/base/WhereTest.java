package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 27.08.12
 * Time: 11:17
 * <p/>
 */
public class WhereTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testWhere(Morphium m) throws Exception {
        log.info("Running with Driver " + m.getDriver().getName());

        try(m) {
            createUncachedObjects(m,100);
            // Thread.sleep(500);
            Query<UncachedObject> q = m.createQueryFor(UncachedObject.class);
            q = q.where("this.counter > 15");
            List<UncachedObject> lst = q.asList();
            assertEquals(85, lst.size(), "wrong number of results");
            assertEquals(85, q.countAll(), "Count wrong");
        }
    }

}
