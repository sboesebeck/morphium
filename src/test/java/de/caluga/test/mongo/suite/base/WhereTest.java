package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import javax.script.ScriptEngineManager;

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

        if (m.getDriver().getName().equals(InMemoryDriver.driverName)) {
            var mgr = new ScriptEngineManager();

            if (mgr.getEngineByExtension("js") == null) {
                log.error("No javascript engine available - inMem $where not running...");
                return;
            }
        }

        try (m) {
            createUncachedObjects(m, 100);
            Thread.sleep(500);
            Query<UncachedObject> q = m.createQueryFor(UncachedObject.class);
            q = q.where("this.counter > 15");
            List<UncachedObject> lst = q.asList();
            assertEquals(85, lst.size(), "wrong number of results for " + m.getDriver().getName());
            assertEquals(85, q.countAll(), "Count wrong for " + m.getDriver().getName());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void whereTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        if (morphium.getDriver().getName().equals(InMemoryDriver.driverName)) {
            var mgr = new ScriptEngineManager();

            if (mgr.getEngineByExtension("js") == null) {
                log.error("No javascript engine available - inMem $where not running...");
                return;
            }
        }

        try (morphium) {
            for (int i = 1; i <= 100; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.where("this.counter<10").f("counter").gt(5);
            log.info(q.toQueryObject().toString());
            List<UncachedObject> lst = q.asList();

            for (UncachedObject o : lst) {
                assertThat(o.getCounter()).describedAs("Counter should be >5 and <10 but is: %d", o.getCounter()).isLessThan(10).isGreaterThan(5);
            }

            assert(morphium.getStatistics().get("X-Entries for: idCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";
        }
    }
}
