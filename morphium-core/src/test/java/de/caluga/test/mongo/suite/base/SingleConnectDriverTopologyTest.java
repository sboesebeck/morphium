package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MOR-068: the {@code SingleMongoConnectDriver} connects to a standalone MongoDB as well as to a replica set. On a standalone server the answer to {@code hello} has no {@code hosts}
 * list, and the driver used to throw a NullPointerException while it compared its host seed with that list: every attempt to connect ended in "max retries exceeded".
 * Run with {@code -Dmorphium.driver=single -Dmorphium.includeSingle=true -Dmorphium.uri=mongodb://...} against a standalone server and against a replica set; without the
 * single connect driver in the run the test is skipped.
 */
@Tag("core")
public class SingleConnectDriverTopologyTest extends MultiDriverTestBase {

    @Entity
    public static class Probe {
        @Id
        public String id;
        public String name;
        public int counter;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theSingleConnectDriverConnectsAndWritesWhateverTheTopology(Morphium m) throws Exception {
        try (m) {
            assumeTrue(m.getDriver() instanceof SingleMongoConnectDriver, "needs -Dmorphium.driver=single -Dmorphium.includeSingle=true and a server");
            m.dropCollection(Probe.class);
            Probe p = new Probe();
            p.id = "p1";
            p.name = "n";
            m.store(p);
            m.createQueryFor(Probe.class).f("id").eq("p1").inc("counter", 2);
            m.createQueryFor(Probe.class).f("id").eq("p1").set("name", "changed");
            Probe r = m.createQueryFor(Probe.class).f("id").eq("p1").get();
            assertEquals("changed", r.name);
            assertEquals(2, r.counter);
        }
    }
}
