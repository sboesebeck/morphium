
package de.caluga.test.mongo.suite.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.test.mongo.suite.data.UncachedObject;

public class ExplainTest extends MorphiumTestBase {


    @Test
    public void queryExplainTest() throws Exception {
        createUncachedObjects(100);

        var q=morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42);

        var explain=q.explain(ExplainVerbosity.queryPlanner);

        assertNotNull(explain);
        log.info("Explain: "+Utils.toJsonString(explain));
        assertEquals("1",explain.get("explainVersion"));
        assertNotNull(explain.get("queryPlanner"));
    }

}
