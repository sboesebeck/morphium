package de.caluga.poppydb.netty;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for #244: the createIndexes wire fast path forwarded only unique/name to the
 * driver and silently dropped expireAfterSeconds (TTL), sparse, hidden and partialFilterExpression -
 * so a client creating a TTL index got {ok:1.0} and an index that never expires anything.
 *
 * <p>Seam test in the handler's own package: MongoCommandHandler's constructor is pure field
 * assignment, so it can be built around a real InMemoryDriver with nulls for the netty/replication
 * collaborators this command path does not touch. No server, no sockets - runs in a normal build.
 */
public class CreateIndexesFastPathTest {

    private InMemoryDriver drv;
    private final String db = "createidx_test";
    private final String coll = "docs";

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private MongoCommandHandler handler() {
        return new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017"), true, "localhost:17017",
                0, () -> null);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ttlRegistry() throws Exception {
        Field f = InMemoryDriver.class.getDeclaredField("collectionsWithTtlIndex");
        f.setAccessible(true);
        return (Map<String, Object>) f.get(drv);
    }

    @Test
    public void createIndexesDirect_forwardsTtlOptionSoTheIndexActuallyExpires() throws Exception {
        handler().processCreateIndexesDirect(Doc.of(
            "$db", db,
            "createIndexes", coll,
            "indexes", List.of(Doc.of(
                "key", Doc.of("expireAt", 1),
                "name", "expireAt_1",
                "expireAfterSeconds", 3600))));

        assertTrue(ttlRegistry().containsKey(db + "." + coll),
            "expireAfterSeconds must reach the driver so a TTL index is actually registered");

        ListIndexesCommand li = new ListIndexesCommand(drv).setDb(db).setColl(coll);
        List<IndexDescription> indexes = li.execute();
        IndexDescription created = indexes.stream()
            .filter(i -> "expireAt_1".equals(i.getName())).findFirst().orElse(null);
        assertNotNull(created, "index must exist, got: " + indexes);
        assertEquals(3600, created.getExpireAfterSeconds(), "TTL option must be preserved");
    }

    @Test
    public void createIndexesDirect_forwardsSparseAndOtherOptions() throws Exception {
        handler().processCreateIndexesDirect(Doc.of(
            "$db", db,
            "createIndexes", coll,
            "indexes", List.of(Doc.of(
                "key", Doc.of("value", 1),
                "name", "value_1",
                "unique", true,
                "sparse", true,
                "hidden", true,
                "partialFilterExpression", Doc.of("value", Doc.of("$gt", 5))))));

        ListIndexesCommand li = new ListIndexesCommand(drv).setDb(db).setColl(coll);
        List<IndexDescription> indexes = li.execute();
        IndexDescription created = indexes.stream()
            .filter(i -> "value_1".equals(i.getName())).findFirst().orElse(null);
        assertNotNull(created, "index must exist, got: " + indexes);
        assertEquals(Boolean.TRUE, created.getSparse(), "sparse must not be dropped by the fast path");
        assertEquals(Boolean.TRUE, created.getUnique(), "unique still works");
    }
}
