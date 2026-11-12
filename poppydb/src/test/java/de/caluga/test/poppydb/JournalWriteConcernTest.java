package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.poppydb.PoppyDB;

/**
 * MOR-111: {@link MsgLock} is annotated {@code @WriteSafety(waitForJournalCommit = true, level = MAJORITY)} and PoppyDB
 * has no journal. Every lock write therefore asked for {@code j:true} and was answered with a {@code writeConcernError}
 * (code 2, BadValue) <em>after</em> it had been applied. Since MOR-065 a delete raises such a concern, so releasing the
 * lock of an exclusive message - {@code SingleCollectionMessaging.deleteLock}, a plain {@code Query.remove()} on
 * {@code MsgLock} - failed with a {@code WriteConcernNotSatisfiedException} although the lock was gone.
 *
 * <p>Morphium drops the journal flag for a backend that cannot journal, one warning per instance
 * ({@code MorphiumDriver.hasJournal()}), the same way it downgrades a replica-set write concern on a standalone.
 */
@Tag("server")
public class JournalWriteConcernTest {

    private static final String DB = "journal_write_concern_test";

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try {
                srv.start();
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private Morphium connect(int port) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase(DB);
        cfg.connectionSettings().setMaxConnections(10);
        return new Morphium(cfg);
    }

    @Test
    public void aLockWriteConcernCarriesNoJournalFlagOnPoppyDB() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "localhost", 100, 1);
        startServer(srv, port);
        try (Morphium m = connect(port)) {
            assertTrue(m.getDriver().isPoppyDB(), "sanity: the hello handshake must identify PoppyDB");
            assertTrue(!m.getDriver().hasJournal(), "PoppyDB has no journal");

            WriteConcern wc = m.getWriteConcernForClass(MsgLock.class);
            assertNotNull(wc, "MsgLock carries @WriteSafety, so it has a write concern");
            Map<String, Object> map = wc.asMap();
            assertFalse(Boolean.TRUE.equals(map.get("j")),
                    "j:true must not be sent to a backend without journal - got " + map);
            // the level of @WriteSafety survives; a standalone PoppyDB downgrades w like it does for every entity
            assertTrue(map.get("w") != null, "the concern still carries a w: " + map);
        } finally {
            srv.shutdown();
        }
    }

    @Test
    public void aLockCanBeStoredAndDeletedOnPoppyDB() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "localhost", 100, 1);
        startServer(srv, port);
        try (Morphium m = connect(port)) {
            MorphiumId msgId = new MorphiumId();
            MsgLock lock = new MsgLock(msgId);
            lock.setLockId("procA");
            m.store(lock);
            assertNotNull(m.createQueryFor(MsgLock.class).f("_id").eq(msgId).get(), "the lock must be stored");

            // the production path of SingleCollectionMessaging.deleteLock: a plain remove on MsgLock
            m.createQueryFor(MsgLock.class).setCollectionName(m.getMapper().getCollectionName(MsgLock.class))
                    .f("_id").eq(msgId).f("lock_id").eq("procA").remove();

            assertNull(m.createQueryFor(MsgLock.class).f("_id").eq(msgId).get(),
                    "the lock must be gone - and the release must not raise a write concern error");
        } finally {
            srv.shutdown();
        }
    }
}
