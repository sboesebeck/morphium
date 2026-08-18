package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MongoDB's default read preference is primary. A read that reaches a secondary WITHOUT an
 * explicit $readPreference must therefore be rejected (13435 NotPrimaryNoSecondaryOk), exactly
 * like mongod treats a direct connection without secondaryOk. Previously only an explicit
 * mode:"primary" was rejected - a preference-less read silently served possibly-stale data.
 * Morphium's own wire commands always carry $readPreference (default primaryPreferred), so
 * they are unaffected.
 */
public class SecondaryReadPreferenceTest {

    private InMemoryDriver drv;

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

    private MongoCommandHandler handler(boolean primary) {
        return new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017", "localhost:17018"),
                primary, "localhost:17018", 0, () -> null);
    }

    private MongoCommandHandler.CheckResult dispatch(MongoCommandHandler h, Map<String, Object> doc) {
        EmbeddedChannel ch = new EmbeddedChannel(h);
        try {
            ChannelHandlerContext hctx = ch.pipeline().context(MongoCommandHandler.class);
            return h.preDispatch(hctx, "find", doc);
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void secondaryRejectsReadWithoutReadPreference() {
        MongoCommandHandler.CheckResult res = dispatch(handler(false),
                Doc.of("$db", "db", "find", "coll"));
        assertTrue(res.rejected(), "no $readPreference means primary - a secondary must reject the read");
        assertEquals(13435, ((Number) res.errorResponse.get("code")).intValue());
    }

    @Test
    public void secondaryRejectsExplicitPrimaryMode() {
        MongoCommandHandler.CheckResult res = dispatch(handler(false),
                Doc.of("$db", "db", "find", "coll", "$readPreference", Doc.of("mode", "primary")));
        assertTrue(res.rejected());
        assertEquals(13435, ((Number) res.errorResponse.get("code")).intValue());
    }

    @Test
    public void secondaryAcceptsSecondaryCompatibleModes() {
        for (String mode : List.of("primaryPreferred", "secondary", "secondaryPreferred", "nearest")) {
            MongoCommandHandler.CheckResult res = dispatch(handler(false),
                    Doc.of("$db", "db", "find", "coll", "$readPreference", Doc.of("mode", mode)));
            assertFalse(res.rejected(), "mode " + mode + " must be readable on a secondary");
        }
    }

    @Test
    public void primaryAcceptsReadWithoutReadPreference() {
        MongoCommandHandler.CheckResult res = dispatch(handler(true),
                Doc.of("$db", "db", "find", "coll"));
        assertFalse(res.rejected());
    }
}
