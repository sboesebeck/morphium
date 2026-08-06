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
    private static final String LOCK_ID = "migration_lock";

    private final Morphium morphium;
    private final MorphiumMigrationConfig config;

    /** Owner identifier for this runner instance, set during {@link #acquireLock()}. */
    private String currentOwner;

    public MorphiumMigrationRunner(Morphium morphium, MorphiumMigrationConfig config) {
        this.morphium = morphium;
        this.config = config;
        validateConfig();
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
                // concurrently. Owner-guarded, so it silently becomes a no-op once another
                // process has already taken over the lock -- this instance's subsequent writes
                // and the final releaseLock() are then no-ops too (see releaseLock()'s owner
                // check).
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

        try {
            invokeMigrationMethod(migration.execMethod(), instance);
            long elapsed = System.currentTimeMillis() - startTime;
            recordExecution(migration, elapsed, MorphiumMigrationEntry.ChangeState.EXECUTED);
            log.info("Migration {} completed in {}ms", migration.changeId(), elapsed);

        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
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
     * {@code owner=currentOwner} so it becomes a silent no-op if another process has already
     * taken over the lock (e.g. because a previous renewal round-trip was slow enough for the
     * old TTL to expire first). Called after every executed migration by {@link #runMigrations}
     * — see the call site for why a heartbeat is needed at all.
     */
    private void renewLock() {
        Date expiresAt = new Date(System.currentTimeMillis() + config.lockTtlSeconds() * 1000L);
        Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
        q.setCollectionName(config.lockCollection());
        q.f("_id").eq(LOCK_ID);
        q.f("owner").eq(currentOwner);
        try {
            q.set(Map.of("expires_at", expiresAt), false, false);
        } catch (Exception e) {
            // Best-effort: if the renewal round-trip itself fails, the original TTL still
            // applies and acquireLock()'s next caller will simply see an expired lock sooner
            // than expected. Not fatal to the migration run in progress.
            log.warn("Failed to renew migration lock (owner={})", currentOwner, e);
        }
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
