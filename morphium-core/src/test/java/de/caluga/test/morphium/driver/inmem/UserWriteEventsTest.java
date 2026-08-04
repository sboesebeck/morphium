package de.caluga.test.morphium.driver.inmem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.inmem.auth.ScramCredentials;
import de.caluga.morphium.driver.inmem.auth.UserDocuments;
import de.caluga.test.mongo.suite.base.TestUtils;

/**
 * User writes (createUser / updateUser) in the in-memory driver must be visible to change
 * streams exactly like any other write - PoppyDB's ReplicationManager watches the cluster
 * ("admin" db, empty pipeline, fullDocument updateLookup - see {@link #subscribeClusterWatch()})
 * to replicate admin.system.users between replica-set nodes, so a user document that never emits
 * a change event would silently never propagate. Also covers the new mongod-compatible
 * updateUser command that those events depend on.
 */
@Tag("inmemory")
public class UserWriteEventsTest {

    private InMemoryDriver drv;

    @BeforeEach
    void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private Map<String, Object> createUser(String user, String pwd) throws Exception {
        CreateUserAdminCommand cmd = new CreateUserAdminCommand(null).setUserName(user).setPwd(pwd);
        cmd.setDb("admin");
        Map<String, Object> result = drv.readSingleAnswer(drv.runCommand(cmd));
        assertThat(result.get("ok")).as("createUser result: " + result).isEqualTo(1.0);
        return result;
    }

    private Map<String, Object> updateUser(Map<String, Object> rawCommand) throws Exception {
        GenericCommand cmd = new GenericCommand(drv);
        cmd.fromMap(rawCommand);
        return drv.readSingleAnswer(drv.runCommand(cmd));
    }

    /** Collects events delivered to a subscribed watch, plus the machinery to stop it. */
    private static class ClusterWatch {
        final List<Map<String, Object>> events = Collections.synchronizedList(new ArrayList<>());
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch registered = new CountDownLatch(1);
        Thread thread;

        void stop() throws InterruptedException {
            running.set(false);
            thread.join(5000);
        }

        Map<String, Object> firstOfType(String operationType) {
            synchronized (events) {
                return events.stream()
                    .filter(e -> operationType.equals(e.get("operationType")))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(
                        "no " + operationType + " event arrived, got: " + events));
            }
        }
    }

    /**
     * Subscribes a cluster-level watch the same way PoppyDB's ReplicationManager does
     * (db "admin", no collection, empty pipeline, fullDocument updateLookup) - the load-bearing
     * contract for user replication: this is exactly the watch that must receive user writes.
     */
    private ClusterWatch subscribeClusterWatch() throws InterruptedException {
        ClusterWatch cw = new ClusterWatch();
        var con = drv.getPrimaryConnection(null);
        WatchCommand watch = new WatchCommand(con)
            .setDb("admin")
            .setMaxTimeMS(300)
            .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
            .setPipeline(List.of())
            .setRegistrationCallback(cw.registered::countDown)
            .setCb(new DriverTailableIterationCallback() {
                @Override
                public void incomingData(Map<String, Object> data, long dur) {
                    cw.events.add(data);
                }

                @Override
                public boolean isContinued() {
                    return cw.running.get();
                }
            });
        cw.thread = Thread.ofVirtual().start(() -> {
            try {
                watch.watch();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                watch.releaseConnection();
            }
        });
        assertThat(cw.registered.await(5, TimeUnit.SECONDS)).as("watch never registered").isTrue();
        return cw;
    }

    @Test
    void createUserEmitsInsertEventOnAdminSystemUsers() throws Exception {
        ClusterWatch cw = subscribeClusterWatch();
        try {
            createUser("testuser", "pw");
            TestUtils.waitForConditionToBecomeTrue(5000, "no insert event for testuser arrived: " + cw.events,
                () -> cw.events.stream().anyMatch(e -> "insert".equals(e.get("operationType"))));
        } finally {
            cw.stop();
        }

        Map<String, Object> event = cw.firstOfType("insert");
        @SuppressWarnings("unchecked")
        Map<String, Object> ns = (Map<String, Object>) event.get("ns");
        assertThat(ns.get("db")).isEqualTo("admin");
        assertThat(ns.get("coll")).isEqualTo("system.users");
        @SuppressWarnings("unchecked")
        Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");
        assertThat(fullDoc).as("insert event must carry fullDocument").isNotNull();
        assertThat(fullDoc.get("user")).isEqualTo("testuser");
    }

    @Test
    void updateUserReplacesDocumentAndEmitsReplaceEvent() throws Exception {
        createUser("u1", "oldpw");
        ClusterWatch cw = subscribeClusterWatch();
        Map<String, Object> result;
        try {
            result = updateUser(Doc.of("updateUser", "u1", "pwd", "newpw", "$db", "admin"));
            TestUtils.waitForConditionToBecomeTrue(5000, "no replace event for u1 arrived: " + cw.events,
                () -> cw.events.stream().anyMatch(e -> "replace".equals(e.get("operationType"))));
        } finally {
            cw.stop();
        }

        assertThat(result.get("ok")).as("updateUser result: " + result).isEqualTo(1.0);
        Map<String, Object> event = cw.firstOfType("replace");
        @SuppressWarnings("unchecked")
        Map<String, Object> ns = (Map<String, Object>) event.get("ns");
        assertThat(ns.get("db")).isEqualTo("admin");
        assertThat(ns.get("coll")).isEqualTo("system.users");
        @SuppressWarnings("unchecked")
        Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");
        assertThat(fullDoc).as("replace event must carry fullDocument").isNotNull();
        assertThat(fullDoc.get("user")).isEqualTo("u1");
    }

    @Test
    void updateUserChangesScramCredentials() throws Exception {
        createUser("u2", "oldpw");
        var before = drv.findByFieldValue("admin", "system.users", "_id", "admin.u2");
        assertThat(before).hasSize(1);
        @SuppressWarnings("unchecked")
        Map<String, Object> credsBefore = (Map<String, Object>) before.get(0).get("credentials");
        @SuppressWarnings("unchecked")
        Map<String, Object> scram256Before = (Map<String, Object>) credsBefore.get("SCRAM-SHA-256");
        assertThat(scram256Before).as("createUser must store SCRAM-SHA-256 credentials").isNotNull();

        Map<String, Object> result = updateUser(Doc.of("updateUser", "u2", "pwd", "newpw", "$db", "admin"));
        assertThat(result.get("ok")).as("updateUser result: " + result).isEqualTo(1.0);

        var after = drv.findByFieldValue("admin", "system.users", "_id", "admin.u2");
        assertThat(after).hasSize(1);
        @SuppressWarnings("unchecked")
        Map<String, Object> credsAfter = (Map<String, Object>) after.get(0).get("credentials");
        @SuppressWarnings("unchecked")
        Map<String, Object> scram256After = (Map<String, Object>) credsAfter.get("SCRAM-SHA-256");
        assertThat(scram256After).as("updateUser must keep storing SCRAM-SHA-256 credentials").isNotNull();

        assertThat(scram256After.get("storedKey")).as("storedKey must change with the password")
            .isNotEqualTo(scram256Before.get("storedKey"));
        assertThat(scram256After.get("salt")).as("salt must be freshly generated per update")
            .isNotEqualTo(scram256Before.get("salt"));

        @SuppressWarnings("unchecked")
        List<Object> rolesAfter = (List<Object>) after.get(0).get("roles");
        assertThat(rolesAfter).as("roles preserved when not passed to updateUser").isEmpty();
    }

    @Test
    void updateUserUnknownUserIsCode11() throws Exception {
        Map<String, Object> result = updateUser(
            Doc.of("updateUser", "missing-user", "pwd", "whatever", "$db", "admin"));

        assertThat(result.get("ok")).isEqualTo(0.0);
        assertThat((Integer) result.get("code")).isEqualTo(11);
        assertThat(result.get("codeName")).isEqualTo("UserNotFound");
    }

    @Test
    void updateUserWithoutPwdAndRolesIsBadValue() throws Exception {
        Map<String, Object> result = updateUser(Doc.of("updateUser", "x", "$db", "admin"));

        assertThat(result.get("ok")).isEqualTo(0.0);
        assertThat((Integer) result.get("code")).isEqualTo(2);
        assertThat(result.get("codeName")).isEqualTo("BadValue");
    }

    @Test
    void updateUserWithUnknownMechanismIsBadValue() throws Exception {
        createUser("u3", "oldpw");

        Map<String, Object> result = updateUser(
            Doc.of("updateUser", "u3", "pwd", "npw", "mechanisms", List.of("BOGUS"), "$db", "admin"));

        assertThat(result.get("ok")).isEqualTo(0.0);
        assertThat((Integer) result.get("code")).isEqualTo(2);
        assertThat(result.get("codeName")).isEqualTo("BadValue");
    }

    @Test
    void updateUserRolesOnlyKeepsCredentials() throws Exception {
        createUser("u4", "oldpw");
        var before = drv.findByFieldValue("admin", "system.users", "_id", "admin.u4");
        assertThat(before).hasSize(1);
        Object idBefore = before.get(0).get("_id");
        @SuppressWarnings("unchecked")
        Map<String, Object> credsBefore = (Map<String, Object>) before.get(0).get("credentials");
        assertThat(credsBefore).as("createUser must store credentials").isNotNull();

        ClusterWatch cw = subscribeClusterWatch();
        List<Object> newRoles = List.of(Doc.of("role", "readWrite", "db", "testdb"));
        Map<String, Object> result;
        try {
            result = updateUser(Doc.of("updateUser", "u4", "roles", newRoles, "$db", "admin"));
            TestUtils.waitForConditionToBecomeTrue(5000, "no replace event for u4 arrived: " + cw.events,
                () -> cw.events.stream().anyMatch(e -> "replace".equals(e.get("operationType"))));
        } finally {
            cw.stop();
        }

        assertThat(result.get("ok")).as("updateUser result: " + result).isEqualTo(1.0);

        var after = drv.findByFieldValue("admin", "system.users", "_id", "admin.u4");
        assertThat(after).hasSize(1);
        assertThat(after.get(0).get("_id")).as("_id must not change").isEqualTo(idBefore);
        @SuppressWarnings("unchecked")
        Map<String, Object> credsAfter = (Map<String, Object>) after.get(0).get("credentials");
        assertThat(credsAfter).as("credentials must be unchanged when pwd is not passed").isEqualTo(credsBefore);
        assertThat(after.get(0).get("roles")).as("roles must be replaced").isEqualTo(newRoles);
    }

    /**
     * TOCTOU regression: updateUserInternal used to resolve the target document once,
     * before taking the write lock, and hand that stale reference to both the remove
     * and (in the roles-only path) the replacement build. Under concurrent updateUser
     * calls for the same user, every caller reads the same pre-lock snapshot, so only
     * one {@code users.remove(existing)} actually removes anything - the rest silently
     * miss and every {@code add(replacement)} still runs, leaving several documents
     * with the same {@code _id} in admin.system.users. Fixed by re-resolving the
     * current document by _id inside the write lock. @RepeatedTest gives the race
     * room to reproduce across a JVM warm-up / JIT range.
     */
    @RepeatedTest(10)
    void updateUserConcurrentlyNeverDuplicates() throws Exception {
        createUser("u5", "initial");

        int n = 8;
        CyclicBarrier barrier = new CyclicBarrier(n);
        List<String> passwords = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            passwords.add("pw-" + i + "-" + System.nanoTime());
        }
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();

        for (String pwd : passwords) {
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                    Map<String, Object> result = updateUser(Doc.of("updateUser", "u5", "pwd", pwd, "$db", "admin"));
                    if (!Double.valueOf(1.0).equals(result.get("ok"))) {
                        errors.add(new AssertionError("updateUser failed: " + result));
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join(10_000);
        }

        assertThat(errors).as("updateUser threads must not fail: " + errors).isEmpty();

        var docs = drv.findByFieldValue("admin", "system.users", "_id", "admin.u5");
        assertThat(docs).as("exactly one user document must exist for admin.u5, found: " + docs).hasSize(1);

        Map<String, Object> doc = docs.get(0);
        ScramCredentials creds = UserDocuments.extractCredentials(doc, "SCRAM-SHA-256");
        assertThat(creds).as("updated user must still have SCRAM-SHA-256 credentials").isNotNull();

        long acceptedCount = passwords.stream()
            .filter(pwd -> matchesPassword(creds, "u5", pwd))
            .count();
        assertThat(acceptedCount)
            .as("exactly one of the %d concurrently-applied passwords must be accepted, got %s", n, acceptedCount)
            .isEqualTo(1);
    }

    private boolean matchesPassword(ScramCredentials creds, String user, String candidatePassword) {
        ScramCredentials derived = ScramCredentials.derive(
            creds.getMechanism(), user, candidatePassword, creds.getSalt(), creds.getIterationCount());
        return Arrays.equals(derived.getStoredKey(), creds.getStoredKey());
    }

    /**
     * TOCTOU regression for createUser: the existence check used to run before the collection
     * write lock was taken, so N concurrent createUser calls for the same user could all pass
     * the check and all insert - leaving several documents with the same {@code _id} in
     * admin.system.users. Contract: exactly one caller wins (ok:1.0), every other caller gets
     * code 51003, and exactly one document exists afterwards.
     */
    @RepeatedTest(10)
    void createUserConcurrentlyExactlyOneWins() throws Exception {
        int n = 8;
        CyclicBarrier barrier = new CyclicBarrier(n);
        List<Map<String, Object>> results = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                    CreateUserAdminCommand cmd = new CreateUserAdminCommand(null)
                        .setUserName("u6").setPwd("pw-" + idx);
                    cmd.setDb("admin");
                    results.add(drv.readSingleAnswer(drv.runCommand(cmd)));
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join(10_000);
        }

        assertThat(errors).as("createUser threads must not fail: " + errors).isEmpty();
        assertThat(results).hasSize(n);

        long okCount = results.stream().filter(r -> Double.valueOf(1.0).equals(r.get("ok"))).count();
        long duplicateCount = results.stream().filter(r -> Integer.valueOf(51003).equals(r.get("code"))).count();
        assertThat(okCount)
            .as("exactly one concurrent createUser must win, results: " + results)
            .isEqualTo(1);
        assertThat(duplicateCount)
            .as("every losing createUser must get code 51003, results: " + results)
            .isEqualTo(n - 1);

        var docs = drv.findByFieldValue("admin", "system.users", "_id", "admin.u6");
        assertThat(docs).as("exactly one user document must exist for admin.u6, found: " + docs).hasSize(1);
    }

    private static String resumeTokenData(Map<String, Object> event) {
        @SuppressWarnings("unchecked")
        Map<String, Object> id = (Map<String, Object>) event.get("_id");
        return (String) id.get("_data");
    }

    private long countReplaceEvents(ClusterWatch cw) {
        synchronized (cw.events) {
            return cw.events.stream().filter(e -> "replace".equals(e.get("operationType"))).count();
        }
    }

    /**
     * Store-order vs stream-order regression: user writes used to release the collection write
     * lock BEFORE calling notifyWatchers, so under two concurrent password changes the store
     * order A→B could get its change-stream tokens assigned as B→A. A replica-set secondary
     * applies stream events in token order as _id-keyed upserts, so it would converge on the
     * OLD document. Contract: among each round's replace events, the one with the HIGHEST
     * resume token carries the FINAL stored document.
     *
     * <p>This race has no deterministic seam (the inversion window is the gap between
     * writeLock.unlock() and the token assignment inside notifyWatchers), so this test is
     * probabilistic: 50 barrier-started rounds of concurrent roles-only updateUser (roles-only
     * deliberately - a pwd change spends ~15ms in SCRAM key derivation INSIDE the write lock,
     * which makes the losing thread's microsecond-scale post-unlock window practically
     * unhittable; roles-only keeps both sides' windows symmetric). Pre-fix it fails
     * intermittently (RED-flaky); post-fix the dedicated emit lock makes token order equal
     * store order, so it must always pass.
     */
    @Test
    void concurrentUpdateUserHighestTokenEventCarriesFinalDocument() throws Exception {
        createUser("u7", "initial");
        ClusterWatch cw = subscribeClusterWatch();
        try {
            int writers = 8;
            for (int round = 0; round < 50; round++) {
                CyclicBarrier barrier = new CyclicBarrier(writers);
                List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
                List<Thread> threads = new ArrayList<>();

                for (int i = 0; i < writers; i++) {
                    List<Object> roles = List.of(Doc.of("role", "role-" + round + "-" + i, "db", "testdb"));
                    Thread t = new Thread(() -> {
                        try {
                            barrier.await();
                            Map<String, Object> r = updateUser(Doc.of("updateUser", "u7", "roles", roles, "$db", "admin"));
                            if (!Double.valueOf(1.0).equals(r.get("ok"))) {
                                errors.add(new AssertionError("updateUser failed: " + r));
                            }
                        } catch (Throwable e) {
                            errors.add(e);
                        }
                    });
                    threads.add(t);
                    t.start();
                }
                for (Thread t : threads) {
                    t.join(10_000);
                }
                assertThat(errors).as("round " + round + ": updateUser threads must not fail: " + errors).isEmpty();

                long expected = (long) (round + 1) * writers;
                TestUtils.waitForConditionToBecomeTrue(5000,
                    "round " + round + ": expected " + expected + " replace events, got " + countReplaceEvents(cw),
                    () -> countReplaceEvents(cw) >= expected);

                var docs = drv.findByFieldValue("admin", "system.users", "_id", "admin.u7");
                assertThat(docs).as("round " + round + ": exactly one document expected, found: " + docs).hasSize(1);
                Object storedRoles = docs.get(0).get("roles");

                Map<String, Object> latestEvent;
                synchronized (cw.events) {
                    latestEvent = cw.events.stream()
                        .filter(e -> "replace".equals(e.get("operationType")))
                        .max(java.util.Comparator.comparing(UserWriteEventsTest::resumeTokenData))
                        .orElseThrow();
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> fullDoc = (Map<String, Object>) latestEvent.get("fullDocument");
                assertThat(fullDoc).as("round " + round + ": replace event must carry fullDocument").isNotNull();
                assertThat(fullDoc.get("roles"))
                    .as("round %d: the highest-token replace event must carry the FINAL stored document "
                        + "(stream order diverged from store order)", round)
                    .isEqualTo(storedRoles);
            }
        } finally {
            cw.stop();
        }
    }
}
