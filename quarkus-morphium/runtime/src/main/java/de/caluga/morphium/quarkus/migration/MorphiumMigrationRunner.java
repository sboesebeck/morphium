/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.quarkus.migration;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Executes pending database migrations defined by {@link MorphiumChangeUnit} classes.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Acquire a distributed lock ({@code morphiumMigrationLock} collection)</li>
 *   <li>Load already-executed migrations from the changelog</li>
 *   <li>Discover and sort pending migrations by {@link MorphiumChangeUnit#order()}</li>
 *   <li>Execute each pending migration's {@link Execution} method</li>
 *   <li>Record success/failure in the changelog</li>
 *   <li>Release the lock</li>
 * </ol>
 */
public class MorphiumMigrationRunner {

    private static final Logger log = LoggerFactory.getLogger(MorphiumMigrationRunner.class);

    /**
     * The fixed {@code _id} of the single migration-lock document. Package-visible (not
     * {@code private}) and named consistently with the rest of the class so that tests in
     * other packages needing the real lock-document id (e.g. to simulate a held lock) can
     * reference this constant instead of duplicating it as a copy-pasted string literal --
     * a literal that would silently go stale and make such a test blind to its own bugs the
     * moment this constant is renamed. Exposed via {@link #getLockId()} rather than made
     * {@code public} directly, keeping the field itself an implementation detail while still
     * giving test code (which lives in a different package, {@code
     * de.caluga.morphium.quarkus.it}) a single, refactor-safe source of truth.
     */
    static final String LOCK_ID = "migration_lock";

    /**
     * The in-flight lock heartbeat (see {@link #startLockHeartbeat}) wakes up roughly this many
     * times per {@code lockTtlSeconds} window (subject to {@link #HEARTBEAT_MIN_INTERVAL_MS}),
     * so the lock is renewed comfortably before it could expire even while a single change unit
     * is still running. E.g. with the default {@code lockTtlSeconds=60} this fires every ~20s.
     */
    private static final int HEARTBEAT_TICKS_PER_TTL = 3;

    /**
     * Lower bound for the in-flight heartbeat's tick interval, in milliseconds.
     *
     * <p>Purpose of the floor: purely to cap DB load for very small {@code lockTtlSeconds} --
     * without it, e.g. {@code lockTtlSeconds=1} with {@code HEARTBEAT_TICKS_PER_TTL=3} would
     * otherwise tick every ~333ms, which is already fine, but even smaller TTLs could drive the
     * interval towards zero and hammer the lock collection. It must NOT, however, be so large
     * that it eats into (or exceeds) the TTL window itself for realistic {@code lockTtlSeconds}
     * values: a previous version of this floor was 1000ms flat, which for {@code
     * lockTtlSeconds=1} produced an interval EQUAL to the TTL -- i.e. the first heartbeat tick
     * was scheduled to land exactly when the lock was already expiring, with zero margin for
     * thread-start latency, GC pauses, or the {@code renewLock()} round-trip itself. That made
     * the in-flight heartbeat unable to ever renew in time for small TTLs, which is a real user
     * -facing bug (anyone configuring a short {@code lockTtlSeconds}, not just tests) and not
     * merely a test-timing artifact.
     *
     * <p>200ms is chosen as a floor that is small enough to still leave several ticks (and
     * therefore several renewal attempts with real safety margin) inside a 1s TTL, while still
     * being coarse enough that normal-to-large TTLs (seconds to minutes) are completely
     * unaffected -- {@code HEARTBEAT_TICKS_PER_TTL}'s natural interval already exceeds 200ms for
     * any {@code lockTtlSeconds >= 1}, so the floor only ever engages for sub-second TTLs.
     */
    private static final long HEARTBEAT_MIN_INTERVAL_MS = 200L;

    private final Morphium morphium;
    private final MorphiumMigrationConfig config;

    /** Owner identifier for this runner instance, set during {@link #acquireLock()}. */
    private String currentOwner;

    /**
     * Set by the in-flight lock heartbeat (see {@link #startLockHeartbeat}) if it detects,
     * while a single change unit is still running, that the lock has been taken over by
     * another process. Read and cleared by {@link #executeMigration} right after the unit
     * finishes so the failure is never silently swallowed -- it is always either the primary
     * exception thrown from {@code executeMigration}, or attached as a suppressed exception on
     * the migration's own failure if both happened.
     */
    private final AtomicReference<RuntimeException> heartbeatFailure = new AtomicReference<>();

    public MorphiumMigrationRunner(Morphium morphium, MorphiumMigrationConfig config) {
        this.morphium = morphium;
        this.config = config;
        validateConfig();
    }

    /**
     * Returns the fixed {@code _id} of the migration-lock document used by this runner.
     * Intended for tests (and diagnostic tooling) that need to reason about the lock document
     * directly -- e.g. to simulate a held lock -- without duplicating {@link #LOCK_ID} as a
     * copy-pasted string literal that would silently go stale if the constant is ever renamed.
     */
    public static String getLockId() {
        return LOCK_ID;
    }

    /**
     * Runs all pending migrations from the given list of change-unit class names.
     *
     * @param changeUnitClassNames fully qualified class names of {@link MorphiumChangeUnit} classes
     * @throws RuntimeException if a migration fails
     */
    public void execute(List<String> changeUnitClassNames) {
        if (changeUnitClassNames == null || changeUnitClassNames.isEmpty()) {
            log.info("No @MorphiumChangeUnit classes found — skipping migrations");
            return;
        }

        List<MigrationInfo> migrations = resolveMigrations(changeUnitClassNames);
        if (migrations.isEmpty()) {
            log.info("No valid @MorphiumChangeUnit classes found — skipping migrations");
            return;
        }

        validateUniqueIds(migrations);
        migrations.sort(MorphiumMigrationRunner::compareByOrder);
        log.info("Found {} migration(s) to evaluate", migrations.size());

        acquireLockWithWait();
        try {
            Set<String> executedIds = loadExecutedChangeIds();
            for (MigrationInfo migration : migrations) {
                if (executedIds.contains(migration.changeId())) {
                    log.debug("Skipping already executed migration: {} ({})", migration.changeId(), migration.className());
                    continue;
                }
                executeMigration(migration);
                // Renew the lock's TTL after every executed migration: without this, a
                // migration run that takes longer than lockTtlSeconds lets a second instance
                // atomically steal the lock (acquireLock()'s expires_at <= now condition would
                // match) and start running the SAME still-in-progress change units
                // concurrently. Owner-guarded -- but NOT a silent no-op if another process has
                // already taken over: renewLock() inspects the owner-guarded update's matched
                // count ("n"), exactly like acquireLock() does, and throws when it is 0,
                // aborting this run immediately instead of continuing. This matters because
                // only releaseLock() is owner-guarded against a lost lock -- recordExecution()
                // and the change units themselves are NOT, so silently continuing here would
                // let this instance keep writing (changelog entries, change-unit side effects)
                // concurrently with whatever process now legitimately owns the lock. This call
                // renews between change units; the separate in-flight heartbeat started inside
                // executeMigration() additionally renews WHILE a single change unit is still
                // executing, closing the gap where one unit alone runs longer than
                // lockTtlSeconds.
                renewLock();
            }
        } finally {
            releaseLock();
        }

        log.info("All migrations completed successfully");
    }

    // ------------------------------------------------------------------
    // Configuration validation
    // ------------------------------------------------------------------

    private void validateConfig() {
        if (config.lockTtlSeconds() <= 0) {
            throw new IllegalArgumentException(
                    "quarkus.morphium.migration.lock-ttl-seconds must be > 0, got: " + config.lockTtlSeconds());
        }
    }

    private void validateUniqueIds(List<MigrationInfo> migrations) {
        Set<String> seen = new HashSet<>();
        for (MigrationInfo m : migrations) {
            if (m.changeId() == null || m.changeId().isBlank()) {
                throw new IllegalStateException("@MorphiumChangeUnit " + m.className()
                        + " has an empty id — a non-blank id is required.");
            }
            if (!seen.add(m.changeId())) {
                throw new IllegalStateException("Duplicate @MorphiumChangeUnit id '"
                        + m.changeId() + "' — each migration must have a unique id.");
            }
        }
    }

    /**
     * Compares two migrations by {@link MorphiumChangeUnit#order()} numerically when both
     * values parse as a {@code long}, falling back to a plain lexicographic string comparison
     * otherwise.
     *
     * <p>{@code order()} is a {@code String}, not a number, so {@code Comparator.comparing}
     * on it directly sorts lexicographically: {@code "10"} sorts BEFORE {@code "2"} (because
     * {@code '1' < '2'} as characters), silently reordering migrations once there are more than
     * 9 of them unless every {@code order} value happens to be zero-padded to the same width
     * (the convention every migration in this codebase's own tests already follows, which is
     * exactly why this was never caught by them). Falling back to lexicographic comparison for
     * non-numeric values keeps this compatible with a date-based or other non-numeric ordering
     * convention some users may already rely on.
     */
    static int compareByOrder(MigrationInfo a, MigrationInfo b) {
        Long numA = tryParseLong(a.order());
        Long numB = tryParseLong(b.order());
        if (numA != null && numB != null) {
            return Long.compare(numA, numB);
        }
        return a.order().compareTo(b.order());
    }

    private static Long tryParseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Migration resolution
    // ------------------------------------------------------------------

    private List<MigrationInfo> resolveMigrations(List<String> classNames) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<MigrationInfo> result = new ArrayList<>();

        for (String className : classNames) {
            try {
                Class<?> clazz = Class.forName(className, true, cl);
                MorphiumChangeUnit annotation = clazz.getAnnotation(MorphiumChangeUnit.class);
                if (annotation == null) {
                    log.warn("Class {} is not annotated with @MorphiumChangeUnit — skipping", className);
                    continue;
                }

                Method execMethod = findAnnotatedMethod(clazz, Execution.class, true);
                Method rollbackMethod = findAnnotatedMethod(clazz, RollbackExecution.class, false);

                result.add(new MigrationInfo(
                        annotation.id(),
                        annotation.order(),
                        annotation.author(),
                        className,
                        clazz,
                        execMethod,
                        rollbackMethod));

            } catch (ClassNotFoundException e) {
                log.warn("Could not load migration class: {} — skipping", className);
            }
        }

        return result;
    }

    private Method findAnnotatedMethod(Class<?> clazz, Class<? extends java.lang.annotation.Annotation> annotation,
                                       boolean required) {
        Method found = null;
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(annotation)) {
                if (found != null) {
                    throw new IllegalStateException("Class " + clazz.getName()
                            + " has multiple methods annotated with @"
                            + annotation.getSimpleName()
                            + (required ? " — exactly one is required." : " — at most one is allowed."));
                }
                m.setAccessible(true);
                found = m;
            }
        }
        if (found == null && required) {
            throw new IllegalStateException("@MorphiumChangeUnit " + clazz.getName()
                    + " has no @" + annotation.getSimpleName() + " method — exactly one is required.");
        }
        return found;
    }

    // ------------------------------------------------------------------
    // Migration execution
    // ------------------------------------------------------------------

    private void executeMigration(MigrationInfo migration) {
        log.info("Executing migration: {} (order={}, author={})",
                migration.changeId(), migration.order(), migration.author());

        long startTime = System.currentTimeMillis();
        Object instance;
        try {
            instance = migration.clazz().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate migration class " + migration.className()
                    + ". Ensure it has a public no-arg constructor.", e);
        }

        Thread heartbeat = startLockHeartbeat(migration.changeId());
        try {
            try {
                invokeMigrationMethod(migration.execMethod(), instance);
            } catch (Exception e) {
                long elapsed = System.currentTimeMillis() - startTime;
                RuntimeException lost = stopLockHeartbeat(heartbeat);

                if (lost != null) {
                    // The change unit itself also failed (its own exception, e, is the primary
                    // cause below); attach the lock-loss failure as a suppressed exception so
                    // both are visible together instead of losing one of them.
                    e.addSuppressed(lost);
                }

                recordExecution(migration, elapsed, MorphiumMigrationEntry.ChangeState.FAILED);
                log.error("Migration {} failed after {}ms", migration.changeId(), elapsed, e);

                RuntimeException failure = new RuntimeException("Migration " + migration.changeId() + " failed", e);
                if (migration.rollbackMethod() != null) {
                    // If the rollback itself also fails, that failure must not be silently swallowed
                    // (previously only logged) -- the database can be left in an unknown
                    // intermediate state (migration partially applied, rollback partially/not
                    // applied), and losing the rollback failure's details makes that state much
                    // harder to diagnose. Attached as a suppressed exception on the original
                    // migration failure, so both are visible together wherever this exception is
                    // logged or reported, without changing what actually gets thrown (the original
                    // migration failure remains the primary cause, per existing behavior/tests).
                    tryRollback(migration, instance).ifPresent(failure::addSuppressed);
                }

                throw failure;
            }

            // The @Execution method itself completed normally; still need to check whether the
            // heartbeat discovered mid-run that the lock had already been taken over. Handled
            // here, outside the try/catch above, so this lock-loss failure is reported on its
            // own terms instead of being caught and re-wrapped as a generic migration failure.
            long elapsed = System.currentTimeMillis() - startTime;
            RuntimeException lost = stopLockHeartbeat(heartbeat);
            if (lost != null) {
                // The unit itself finished, but the heartbeat detected mid-run that the lock had
                // already been taken over -- treat this exactly like a lock loss detected by
                // renewLock() between units: the run must not continue (recordExecution() below,
                // and any subsequent units, are NOT owner-guarded).
                recordExecution(migration, elapsed, MorphiumMigrationEntry.ChangeState.FAILED);
                throw lost;
            }

            recordExecution(migration, elapsed, MorphiumMigrationEntry.ChangeState.EXECUTED);
            log.info("Migration {} completed in {}ms", migration.changeId(), elapsed);
        } finally {
            stopLockHeartbeat(heartbeat);
        }
    }

    private void invokeMigrationMethod(Method method, Object instance) throws Exception {
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length == 0) {
            method.invoke(instance);
        } else if (paramTypes.length == 1 && Morphium.class.isAssignableFrom(paramTypes[0])) {
            method.invoke(instance, morphium);
        } else {
            throw new IllegalArgumentException("@Execution/@RollbackExecution method " + method.getName()
                    + " must accept either no parameters or a single Morphium parameter");
        }
    }

    /**
     * Attempts to run the migration's {@code @RollbackExecution} method after the migration
     * itself failed, and updates the changelog entry to {@code ROLLED_BACK} on success.
     *
     * @return the rollback's own exception if it also failed, so the caller can attach it
     *         (e.g. as a suppressed exception) to the original migration failure instead of
     *         losing it; {@link Optional#empty()} if the rollback succeeded or there was
     *         nothing to roll back
     */
    private Optional<Exception> tryRollback(MigrationInfo migration, Object instance) {
        try {
            log.info("Attempting rollback for migration: {}", migration.changeId());
            invokeMigrationMethod(migration.rollbackMethod(), instance);
            log.info("Rollback for {} completed successfully", migration.changeId());

            // Update the changelog entry to ROLLED_BACK
            Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
            q.setCollectionName(config.changeLogCollection());
            q.f("_id").eq(migration.changeId());
            MorphiumMigrationEntry entry = q.get();

            if (entry != null) {
                entry.setState(MorphiumMigrationEntry.ChangeState.ROLLED_BACK);
                morphium.store(entry, config.changeLogCollection(), null);
            }
            return Optional.empty();
        } catch (Exception re) {
            log.error("Rollback for {} also failed", migration.changeId(), re);
            return Optional.of(re);
        }
    }

    // ------------------------------------------------------------------
    // Changelog tracking
    // ------------------------------------------------------------------

    /**
     * Loads the set of change IDs that have already been executed successfully.
     * Called once before the migration loop to avoid N+1 queries.
     */
    private Set<String> loadExecutedChangeIds() {
        Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
        q.setCollectionName(config.changeLogCollection());
        q.f("state").eq(MorphiumMigrationEntry.ChangeState.EXECUTED.name());
        return q.asList().stream()
                .map(MorphiumMigrationEntry::getChangeId)
                .collect(Collectors.toSet());
    }

    private void recordExecution(MigrationInfo migration, long executionTimeMs,
                                 MorphiumMigrationEntry.ChangeState state) {
        MorphiumMigrationEntry entry = new MorphiumMigrationEntry();
        entry.setId(migration.changeId());
        entry.setChangeId(migration.changeId());
        entry.setAuthor(migration.author());
        entry.setOrder(migration.order());
        entry.setClassName(migration.className());
        entry.setExecutedAt(new Date());
        entry.setExecutionTimeMs(executionTimeMs);
        entry.setState(state);
        morphium.store(entry, config.changeLogCollection(), null);
    }

    // ------------------------------------------------------------------
    // Distributed lock
    // ------------------------------------------------------------------

    /**
     * Acquires the migration lock, waiting up to {@code lockWaitSeconds} (polling every second)
     * if another instance already holds it, before giving up. With the default
     * {@code lockWaitSeconds=0} this is identical to calling {@link #acquireLock()} directly.
     *
     * <p>Without this, a k8s rolling deployment with multiple replicas crash-loops every replica
     * except the one that happened to win the lock race, until the migration run finishes and
     * the lock is released — instead of the other replicas simply waiting their turn.
     *
     * @throws RuntimeException if the lock is still held by another process after the wait
     */
    private void acquireLockWithWait() {
        long deadline = System.currentTimeMillis() + config.lockWaitSeconds() * 1000L;
        while (true) {
            try {
                acquireLock();
                return;
            } catch (RuntimeException e) {
                if (System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                log.info("Migration lock held by another instance — waiting (owner={})", currentOwner);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    /**
     * Acquires the migration lock atomically using {@code findAndModify} with {@code upsert: true}.
     *
     * <p>The query matches a lock document that either does not exist or has expired.
     * The atomic update sets the new owner and expiration in one round-trip, preventing
     * the race condition where two instances could both read "no lock" and then both write.
     *
     * <p><b>Client clock skew:</b> {@code expires_at} is computed from this process's local
     * clock ({@code System.currentTimeMillis()}), not the MongoDB server's clock. If two
     * instances' clocks drift apart by more than a small fraction of {@code lockTtlSeconds}, the
     * instance with the faster clock can see the other's still-valid lock as already expired and
     * take it over while the original holder is still actively running migrations. Morphium/the
     * MongoDB driver used here has no update-pipeline support for a server-computed expiry
     * (MongoDB 4.2+'s {@code $$NOW} in aggregation-pipeline updates would be the correct
     * primitive, but nothing in this codebase issues one), so this is a real, currently
     * unaddressed limitation, not a false alarm — the accepted mitigation is what every
     * NTP-less distributed lock already requires: keep replica clocks synchronized (NTP/chrony),
     * and set {@code lockTtlSeconds} generously above the expected clock drift, not just above
     * the expected migration runtime.
     *
     * <p>If the lock is held by another process and has not expired, the method throws.
     * Callers that want to wait for a currently-held lock to become available should call
     * {@link #acquireLockWithWait()} instead.
     *
     * @throws RuntimeException if the lock is held by another process
     */
    private void acquireLock() {
        currentOwner = getOwnerIdentifier();
        log.debug("Acquiring migration lock (owner={})", currentOwner);

        Date now = new Date();
        Date expiresAt = new Date(now.getTime() + config.lockTtlSeconds() * 1000L);

        // Atomic: match _id=LOCK_ID where lock is expired (or does not exist via upsert),
        // then $set owner, acquired_at, expires_at in one round-trip.
        Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
        q.setCollectionName(config.lockCollection());
        q.f("_id").eq(LOCK_ID);
        q.f("expires_at").lte(now);

        Map<String, Object> update = Map.of(
                "owner", currentOwner,
                "acquired_at", now,
                "expires_at", expiresAt);

        try {
            var result = q.set(update, true, false);

            // MongoDB returns: n (matched count), nModified, ok, and upserted (array) on upsert.
            // If n==0 and no upsert happened, the lock is held by another process.
            if (result == null) {
                throwLockHeld();
                return;
            }

            Object n = result.get("n");
            Object upserted = result.get("upserted");
            long matchedCount = n instanceof Number num ? num.longValue() : 0;
            boolean wasUpserted = upserted != null;

            if (matchedCount == 0 && !wasUpserted) {
                throwLockHeld();
                return;
            }
        } catch (RuntimeException e) {
            // DuplicateKeyError when _id exists but expires_at condition didn't match (lock still active)
            if (e.getMessage() != null && e.getMessage().contains("duplicate key")) {
                throwLockHeld();
                return;
            }
            throw e;
        }

        log.debug("Migration lock acquired (TTL={}s)", config.lockTtlSeconds());
    }

    /**
     * Extends the lock's {@code expires_at} by another {@code lockTtlSeconds}, guarded by
     * {@code owner=currentOwner}. Called after every executed migration by {@link #execute}
     * — see the call site for why a heartbeat is needed at all.
     *
     * <p>Evaluates the owner-guarded update's matched count ("n"), exactly like
     * {@link #acquireLock()} does: if it is {@code 0}, another process has already taken over
     * the lock (e.g. because a previous renewal round-trip was slow enough for the old TTL to
     * expire first, or a genuine steal happened while this instance was busy). In that case
     * this method throws instead of returning silently, so the caller aborts the migration run
     * rather than continuing to execute change units and write changelog entries concurrently
     * with the new owner.
     *
     * @throws RuntimeException if the lock is no longer held by this instance (matched count 0)
     */
    private void renewLock() {
        Date expiresAt = new Date(System.currentTimeMillis() + config.lockTtlSeconds() * 1000L);
        Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
        q.setCollectionName(config.lockCollection());
        q.f("_id").eq(LOCK_ID);
        q.f("owner").eq(currentOwner);

        Map<String, Object> result;
        try {
            result = q.set(Map.of("expires_at", expiresAt), false, false);
        } catch (Exception e) {
            // Best-effort for a failure of the round-trip itself (e.g. a transient network
            // error): the original TTL still applies, and either the next renewal attempt or
            // acquireLock()'s next caller will simply observe the lock sooner than the full TTL
            // would suggest. This is distinct from -- and less severe than -- an explicit
            // matched-count-0 result below, which proves the lock was DEFINITELY already taken
            // over and must abort the run.
            log.warn("Failed to renew migration lock (owner={})", currentOwner, e);
            return;
        }

        long matchedCount = 0;
        if (result != null) {
            Object n = result.get("n");
            matchedCount = n instanceof Number num ? num.longValue() : 0;
        }

        if (matchedCount == 0) {
            throw new RuntimeException("Migration lock was lost while migrations were still running (owner="
                    + currentOwner + "). Another process has already taken over the lock '" + LOCK_ID
                    + "' in collection '" + config.lockCollection()
                    + "' -- aborting this run to avoid executing change units concurrently with the new owner.");
        }
    }

    // ------------------------------------------------------------------
    // In-flight lock heartbeat
    // ------------------------------------------------------------------

    /**
     * Starts a daemon heartbeat thread that periodically renews the lock for the duration of a
     * single, potentially long-running change unit's {@code @Execution} method.
     *
     * <p>{@link #renewLock()} alone only renews the lock <em>between</em> change units. A
     * single unit that itself runs longer than {@code lockTtlSeconds} (e.g. building an index
     * on a large collection) would otherwise let another instance atomically take over the
     * lock and start running that very same unit concurrently, while the original instance is
     * still inside its (unaware) {@code invoke()} call. This heartbeat closes that gap by
     * renewing on a fixed schedule (roughly {@link #HEARTBEAT_TICKS_PER_TTL} times per TTL
     * window, floored at {@link #HEARTBEAT_MIN_INTERVAL_MS}) for as long as the unit is
     * executing.
     *
     * <p>The thread is a daemon so it can never prevent JVM shutdown by itself, and always
     * terminates via {@link #stopLockHeartbeat} in a {@code finally} block around the unit's
     * execution, so it never outlives the unit it was started for. If a heartbeat tick
     * discovers the lock has been taken over (matched count 0 on the owner-guarded update), it
     * records that as a {@link RuntimeException} in {@link #heartbeatFailure} and stops ticking
     * -- it deliberately does NOT interrupt the running {@code @Execution} method itself (Java
     * has no safe way to abort arbitrary user code), but the caller checks {@code
     * heartbeatFailure} as soon as the unit returns (successfully or not) and surfaces the
     * failure instead of silently accepting the unit's result.
     *
     * @return the heartbeat thread; always non-null, always already started
     */
    private Thread startLockHeartbeat(String changeId) {
        heartbeatFailure.set(null);
        long intervalMs = Math.max(HEARTBEAT_MIN_INTERVAL_MS,
                (config.lockTtlSeconds() * 1000L) / HEARTBEAT_TICKS_PER_TTL);

        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException ie) {
                    return;
                }
                try {
                    renewLock();
                    log.debug("In-flight lock heartbeat renewed lock while executing {} (owner={})",
                            changeId, currentOwner);
                } catch (RuntimeException lockLost) {
                    // Not swallowed: recorded for executeMigration() to pick up and surface as
                    // soon as the (still-running) change unit returns.
                    heartbeatFailure.set(lockLost);
                    log.error("In-flight lock heartbeat detected the migration lock was lost while "
                            + "executing {} (owner={})", changeId, currentOwner, lockLost);
                    return;
                }
            }
        }, "morphium-migration-lock-heartbeat");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Stops the heartbeat thread started by {@link #startLockHeartbeat} and returns whatever
     * lock-loss failure it may have recorded, so the caller can surface it instead of letting
     * it disappear silently. Safe to call more than once for the same thread (e.g. from both
     * the normal-completion path and a {@code finally} block) -- interrupting an already-dead
     * thread, or joining one that already finished, is a no-op.
     */
    private RuntimeException stopLockHeartbeat(Thread heartbeat) {
        heartbeat.interrupt();
        try {
            heartbeat.join(1000L);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return heartbeatFailure.getAndSet(null);
    }

    private void throwLockHeld() {
        // Read the current lock to provide a helpful error message
        Query<MorphiumMigrationLock> readQ = morphium.createQueryFor(MorphiumMigrationLock.class);
        readQ.setCollectionName(config.lockCollection());
        readQ.f("_id").eq(LOCK_ID);
        MorphiumMigrationLock existing = readQ.get();

        String detail = existing != null
                ? "held by '" + existing.getOwner() + "' (acquired at " + existing.getAcquiredAt()
                        + ", expires at " + existing.getExpiresAt() + ")"
                : "in unknown state";

        throw new RuntimeException("Migration lock is " + detail
                + ". If this is stale, wait for TTL expiry or manually remove the lock "
                + "document with _id='" + LOCK_ID + "' from the '"
                + config.lockCollection() + "' collection.");
    }

    /**
     * Releases the migration lock, but only if this runner still owns it.
     * If the lock was overridden (e.g., after TTL expiry by another instance),
     * the lock is not deleted to avoid removing another process's valid lock.
     */
    private void releaseLock() {
        try {
            Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
            q.setCollectionName(config.lockCollection());
            q.f("_id").eq(LOCK_ID);
            q.f("owner").eq(currentOwner);
            morphium.delete(q);
            log.debug("Migration lock released");
        } catch (Exception e) {
            log.warn("Failed to release migration lock", e);
        }
    }

    private String getOwnerIdentifier() {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        return pid + "@" + System.currentTimeMillis();
    }

    // ------------------------------------------------------------------
    // Internal model
    // ------------------------------------------------------------------

    record MigrationInfo(
            String changeId,
            String order,
            String author,
            String className,
            Class<?> clazz,
            Method execMethod,
            Method rollbackMethod
    ) {
    }
}
