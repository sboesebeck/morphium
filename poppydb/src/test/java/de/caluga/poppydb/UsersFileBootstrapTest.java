package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.poppydb.config.ConfigException;
import de.caluga.poppydb.config.UserSpec;
import de.caluga.poppydb.config.UsersFileSpec;

/**
 * E2E coverage for the users-file bootstrap apply (users-file task 4): a
 * {@link UsersFileSpec} handed to {@link PoppyDB#setBootstrapUsers} is applied as an
 * idempotent upsert when the node assumes primary duties - at {@code start()} for
 * non-election nodes, in the leadership hook for election-mode primaries. The optional
 * version gate is backed by the replicated meta document
 * {@code admin.system.version {_id: "poppydb.usersFile", appliedVersion: N}}.
 *
 * Client logins run through morphium's real production client
 * ({@link SingleMongoConnection} with credentials - automatic SCRAM on connect), the same
 * code path {@link AuthTlsWireE2ETest} and {@link UserReplicationTest} exercise: a
 * successful SCRAM login is the proof that the credentials in admin.system.users are the
 * expected ones, because SCRAM verification is node-local.
 *
 * Restart persistence uses the dump/restore path (like production
 * {@code --dump-dir}), so the version-gate tests exercise the real "second boot sees the
 * previously applied state" flow.
 */
public class UsersFileBootstrapTest {

    /** Started nodes, shut down in reverse start order on teardown. */
    private final List<PoppyDB> nodes = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

    // ---- helpers (pattern of UserReplicationTest / AuthTlsWireE2ETest) -------------------

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        nodes.add(srv);
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    /** Poll a condition with generous timeout - replication is asynchronous, never fixed-sleep. */
    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    /**
     * Real SCRAM client login against ONE specific node: SingleMongoConnection performs the
     * SCRAM handshake during connect against the given auth db. True iff the handshake
     * succeeds - i.e. iff the node's local admin.system.users holds matching credentials.
     */
    private boolean scramLoginWorks(int port, String authDb, String user, String password) {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        SingleMongoConnection con = new SingleMongoConnection();
        con.setCredentials(authDb, user, password);
        try {
            con.connect(carrier, "127.0.0.1", port);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                con.close();
            } catch (Exception ignored) {
            }
            try {
                carrier.close();
            } catch (Exception ignored) {
            }
        }
    }

    /** The stored appliedVersion from the node's local meta doc, or null if absent. */
    private Long appliedVersion(PoppyDB srv) throws Exception {
        List<Map<String, Object>> docs = srv.getDriver().find("admin", "system.version",
                Doc.of("_id", "poppydb.usersFile"), null, null, 0, 1);
        if (docs == null || docs.isEmpty()) {
            return null;
        }
        Object v = docs.get(0).get("appliedVersion");
        return v instanceof Number ? ((Number) v).longValue() : null;
    }

    private static UsersFileSpec spec(Long version, UserSpec... users) {
        return new UsersFileSpec(version, List.of(users), List.of());
    }

    private static UserSpec user(String name, String db, String pwd) {
        return new UserSpec(name, db, pwd, List.of(), List.of());
    }

    private PoppyDB singleNode(int port) {
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 20, 60);
        srv.setAuthRequired(true);
        return srv;
    }

    // ---- single node ---------------------------------------------------------------------

    @Test
    public void versionedFileProvisionsUsersAtFirstStart() throws Exception {
        int port = freePort();
        PoppyDB srv = singleNode(port);
        srv.setBootstrapUsers(spec(1L,
                user("app", "admin", "s3cret"),
                new UserSpec("svc", "mydb", "svcpw",
                        List.of(Doc.of("role", "readWrite", "db", "mydb")), List.of())));
        startServer(srv, port);

        assertTrue(scramLoginWorks(port, "admin", "app", "s3cret"),
                "user from the users file must be able to SCRAM-login");
        assertTrue(scramLoginWorks(port, "mydb", "svc", "svcpw"),
                "a user with a non-admin db must authenticate against that db");
        assertFalse(scramLoginWorks(port, "admin", "app", "wrong"),
                "a wrong password must still be rejected");
        assertEquals(1L, appliedVersion(srv),
                "a successful versioned apply must record appliedVersion in admin.system.version");
    }

    @Test
    public void rotationAcrossRestartAppliesBumpedVersion(@TempDir Path dumpDir) throws Exception {
        int port1 = freePort();
        PoppyDB first = singleNode(port1);
        first.setDumpDirectory(dumpDir.toFile());
        first.setBootstrapUsers(spec(1L, user("rotate", "admin", "oldpw")));
        startServer(first, port1);
        assertTrue(scramLoginWorks(port1, "admin", "rotate", "oldpw"));
        first.shutdown(); // final dump persists users + meta doc

        int port2 = freePort();
        PoppyDB second = singleNode(port2);
        second.setDumpDirectory(dumpDir.toFile());
        second.restoreFromDump();
        second.setBootstrapUsers(spec(2L, user("rotate", "admin", "newpw")));
        startServer(second, port2);

        assertTrue(scramLoginWorks(port2, "admin", "rotate", "newpw"),
                "the bumped-version file must rotate the password");
        assertFalse(scramLoginWorks(port2, "admin", "rotate", "oldpw"),
                "the old password must be gone after the rotation");
        assertEquals(2L, appliedVersion(second), "the meta doc must advance to the new version");
    }

    @Test
    public void sameVersionIsNoOp(@TempDir Path dumpDir) throws Exception {
        int port1 = freePort();
        PoppyDB first = singleNode(port1);
        first.setDumpDirectory(dumpDir.toFile());
        first.setBootstrapUsers(spec(5L, user("gate", "admin", "pw1")));
        startServer(first, port1);
        assertTrue(scramLoginWorks(port1, "admin", "gate", "pw1"));
        first.shutdown();

        int port2 = freePort();
        PoppyDB second = singleNode(port2);
        second.setDumpDirectory(dumpDir.toFile());
        second.restoreFromDump();
        // same version, different pwd - a straggler/no-op deploy must NOT touch credentials
        second.setBootstrapUsers(spec(5L, user("gate", "admin", "pw2")));
        startServer(second, port2);

        assertTrue(scramLoginWorks(port2, "admin", "gate", "pw1"),
                "same-version apply must be skipped - stored credentials win");
        assertFalse(scramLoginWorks(port2, "admin", "gate", "pw2"),
                "the skipped file's password must never become valid");
        assertEquals(5L, appliedVersion(second), "the meta doc must stay at the stored version");
    }

    @Test
    public void unversionedFileAlwaysApplies(@TempDir Path dumpDir) throws Exception {
        int port1 = freePort();
        PoppyDB first = singleNode(port1);
        first.setDumpDirectory(dumpDir.toFile());
        first.setBootstrapUsers(spec(null, user("plain", "admin", "p1")));
        startServer(first, port1);
        assertTrue(scramLoginWorks(port1, "admin", "plain", "p1"));
        assertNull(appliedVersion(first), "an unversioned apply must not write a meta doc");
        first.shutdown();

        int port2 = freePort();
        PoppyDB second = singleNode(port2);
        second.setDumpDirectory(dumpDir.toFile());
        second.restoreFromDump();
        second.setBootstrapUsers(spec(null, user("plain", "admin", "p2")));
        startServer(second, port2);

        assertTrue(scramLoginWorks(port2, "admin", "plain", "p2"),
                "an unversioned file must always apply - no gate");
        assertFalse(scramLoginWorks(port2, "admin", "plain", "p1"));
        assertNull(appliedVersion(second), "unversioned applies must still not write a meta doc");
    }

    @Test
    public void unversionedFileAppliesDespiteStoredVersionAndLeavesMetaUntouched(@TempDir Path dumpDir)
            throws Exception {
        int port1 = freePort();
        PoppyDB first = singleNode(port1);
        first.setDumpDirectory(dumpDir.toFile());
        first.setBootstrapUsers(spec(3L, user("mixed", "admin", "pwA")));
        startServer(first, port1);
        assertTrue(scramLoginWorks(port1, "admin", "mixed", "pwA"));
        first.shutdown();

        int port2 = freePort();
        PoppyDB second = singleNode(port2);
        second.setDumpDirectory(dumpDir.toFile());
        second.restoreFromDump();
        second.setBootstrapUsers(spec(null, user("mixed", "admin", "pwB")));
        startServer(second, port2);

        assertTrue(scramLoginWorks(port2, "admin", "mixed", "pwB"),
                "an unversioned file must apply even though a stored appliedVersion exists");
        assertFalse(scramLoginWorks(port2, "admin", "mixed", "pwA"));
        assertEquals(3L, appliedVersion(second),
                "an unversioned apply must leave the stored meta doc untouched");
    }

    @Test
    public void applyFailureAbortsNonElectionStartup() throws Exception {
        int port = freePort();
        PoppyDB srv = singleNode(port);
        nodes.add(srv); // ensure teardown even though start() throws mid-way
        // "PLAIN" is not a supported mechanism -> createUser answers BadValue (not 51003),
        // which is a hard apply failure -> non-election startup must abort.
        srv.setBootstrapUsers(spec(1L,
                new UserSpec("broken", "admin", "topsecretpw", List.of(), List.of("PLAIN"))));

        ConfigException e = assertThrows(ConfigException.class, srv::start,
                "a failed bootstrap apply must abort a non-election startup");
        assertTrue(e.getMessage().contains("broken"),
                "the error should name the failing user: " + e.getMessage());
        assertFalse(e.getMessage().contains("topsecretpw"),
                "the error must never contain the password");
    }

    // ---- CLI wiring ----------------------------------------------------------------------

    @Test
    public void cliWiringLoadsUsersFileAndApplies(@TempDir Path dir) throws Exception {
        Path usersFile = dir.resolve("users.json");
        Files.writeString(usersFile,
                "{\"version\": 4, \"users\": [{\"user\": \"cliuser\", \"pwd\": \"clipw\"}]}");
        Files.setPosixFilePermissions(usersFile, PosixFilePermissions.fromString("rw-------"));

        int port = freePort();
        PoppyDB srv = PoppyDBCLI.configureServer(new String[] {
            "-p", String.valueOf(port), "-b", "127.0.0.1", "--auth",
            "--users-file", usersFile.toString()
        });
        startServer(srv, port);

        assertTrue(scramLoginWorks(port, "admin", "cliuser", "clipw"),
                "--users-file must be loaded by the CLI and applied at startup");
        assertEquals(4L, appliedVersion(srv));
    }

    @Test
    public void cliFailsFastOnBrokenUsersFile(@TempDir Path dir) throws Exception {
        Path usersFile = dir.resolve("users.json");
        Files.writeString(usersFile, "{ this is not json");
        Files.setPosixFilePermissions(usersFile, PosixFilePermissions.fromString("rw-------"));

        assertThrows(ConfigException.class, () -> PoppyDBCLI.configureServer(new String[] {
            "-p", "17017", "-b", "127.0.0.1", "--users-file", usersFile.toString()
        }), "a broken users file must abort before server construction");
    }

    // ---- replica set ---------------------------------------------------------------------

    /**
     * Election-mode RS (pattern of {@link UserReplicationTest#rootUserIsCreatedByPrimaryAndReplicated}):
     * only the designated primary is handed the users file, the secondary has priority 0 (can
     * never become leader) and NO bootstrap spec at all - so it is physically incapable of
     * applying anything locally. If the file's user can SCRAM-login on the secondary and the
     * meta doc shows up there, replication carried both over from the primary's leadership-hook
     * apply.
     */
    @Test
    @Tag("server")
    public void usersAndMetaDocReplicateAndOnlyPrimaryApplies() throws Exception {
        int port1 = freePort();
        int port2 = freePort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        // priority 0 -> secondary can never become a candidate, let alone leader.
        var prio = Map.of("localhost:" + port1, 100, "localhost:" + port2, 0);
        primary.configureReplicaSet("rsUsersFile", hosts, prio, true, null);
        secondary.configureReplicaSet("rsUsersFile", hosts, prio, true, null);

        // Only the future primary knows the users file.
        primary.setBootstrapUsers(spec(7L, user("rsapp", "admin", "rspw")));

        startServer(primary, port1);
        startServer(secondary, port2);
        waitForPrimary(primary);
        waitForInitialSync(secondary);

        assertTrue(poll(15_000, () -> scramLoginWorks(port1, "admin", "rsapp", "rspw")),
                "the leadership hook must apply the users file on the elected primary");
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "admin", "rsapp", "rspw")),
                "the file-provisioned user must replicate to the secondary");
        assertTrue(poll(30_000, () -> Long.valueOf(7L).equals(appliedVersion(secondary))),
                "the version-gate meta doc must replicate to the secondary");
        assertEquals(7L, appliedVersion(primary));
    }

    private void waitForPrimary(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!node.isPrimary() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(node.isPrimary(), "node must become primary");
    }

    @SuppressWarnings("unchecked")
    private boolean initialSyncComplete(PoppyDB node) {
        Object rep = node.getStats().get("replication");
        if (rep instanceof Map) {
            return Boolean.TRUE.equals(((Map<String, Object>) rep).get("initialSyncComplete"));
        }
        return false;
    }

    private void waitForInitialSync(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!initialSyncComplete(node) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(initialSyncComplete(node), "initial sync must complete within 60s");
    }
}
