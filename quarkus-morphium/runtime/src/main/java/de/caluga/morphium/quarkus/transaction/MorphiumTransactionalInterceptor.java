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
package de.caluga.morphium.quarkus.transaction;

import org.jboss.logging.Logger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent.Phase;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

import java.util.concurrent.CompletionStage;

/**
 * CDI interceptor that wraps methods annotated with {@link MorphiumTransactional}
 * in a Morphium transaction.
 *
 * <ul>
 *   <li>Fires {@link Phase#BEFORE_COMMIT} before committing.</li>
 *   <li>Fires {@link Phase#AFTER_COMMIT} after a successful commit.</li>
 *   <li>On exception: aborts, fires {@link Phase#AFTER_ROLLBACK}, re-throws.</li>
 *   <li>On transient MongoDB errors (WriteConflict 112, NoSuchTransaction 251):
 *       retries the entire transaction up to 3 times with exponential backoff.</li>
 *   <li>On CosmosDB: skips transaction wrapping but still fires lifecycle events
 *       ({@code BEFORE_COMMIT}/{@code AFTER_COMMIT} on success, {@code AFTER_ROLLBACK}
 *       on exception) so that observers continue to work. A one-time WARN is logged
 *       at startup and per-call at DEBUG.</li>
 * </ul>
 */
@MorphiumTransactional
@Interceptor
@jakarta.annotation.Priority(Interceptor.Priority.PLATFORM_BEFORE + 200)
public class MorphiumTransactionalInterceptor {

    private static final Logger log = Logger.getLogger(MorphiumTransactionalInterceptor.class);

    @Inject
    Morphium morphium;

    @Inject
    @MorphiumTxPhase(Phase.BEFORE_COMMIT)
    Event<MorphiumTransactionEvent> beforeCommit;

    @Inject
    @MorphiumTxPhase(Phase.AFTER_COMMIT)
    Event<MorphiumTransactionEvent> afterCommit;

    @Inject
    @MorphiumTxPhase(Phase.AFTER_ROLLBACK)
    Event<MorphiumTransactionEvent> afterRollback;

    private volatile Boolean cosmosDb;

    private boolean isCosmosDb() {
        Boolean cached = cosmosDb;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            if (cosmosDb != null) {
                return cosmosDb;
            }
            try {
                cosmosDb = morphium.getDriver().isCosmosDB();
            } catch (Exception e) {
                // Fail-open to false (standard MongoDB) here is intentional, not an oversight:
                // this is a best-effort cache, not the only safety net. If the backend actually
                // IS CosmosDB and this detection call failed only transiently, startTransaction()
                // below throws UnsupportedOperationException on the very next call and that
                // catch block corrects cosmosDb to true immediately -- see its comment. The
                // worst case here is one avoidable failed startTransaction() attempt before
                // self-correcting, never a silently wrong steady state.
                log.warnf("Could not determine if backend is CosmosDB; assuming standard MongoDB. Cause: %s",
                        e.getMessage());
                cosmosDb = false;
            }
            if (cosmosDb) {
                log.warn("CosmosDB detected — @MorphiumTransactional methods will execute "
                        + "WITHOUT transaction wrapping. Individual ops remain atomic; "
                        + "multi-document rollback is unavailable.");
            }
            return cosmosDb;
        }
    }

    @AroundInvoke
    Object aroundInvoke(InvocationContext ctx) throws Throwable {
        Class<?> returnType = ctx.getMethod().getReturnType();
        if (isAsyncReturnType(returnType)) {
            // Fail fast instead of silently doing the wrong thing: ctx.proceed() below returns
            // the CompletionStage/Uni object itself immediately (the method body hasn't
            // actually finished running its async work yet), so committing/firing
            // AFTER_COMMIT right after ctx.proceed() returns would commit the transaction
            // before the method's actual database writes (which typically run later, on
            // repo.getAsyncExecutor() or a Mutiny scheduler) have even happened. There is no
            // reliable way for this synchronous CDI interceptor to hook "when the returned
            // CompletionStage/Uni completes" without materially changing what @MorphiumTransactional
            // does, so this is unsupported until that's built deliberately -- not silently wrong.
            throw new UnsupportedOperationException(
                    "@MorphiumTransactional does not support asynchronous return types (found "
                            + returnType.getName() + " on " + ctx.getMethod().getDeclaringClass().getSimpleName()
                            + "." + ctx.getMethod().getName() + "()). The interceptor commits "
                            + "immediately after ctx.proceed() returns, which happens before an async "
                            + "method's actual work completes -- use a synchronous method (or the "
                            + "repository's doXxxAsync methods called from within a synchronous "
                            + "@MorphiumTransactional method, so their CompletionStage is awaited "
                            + "before the method returns) instead.");
        }

        // CosmosDB: execute without transaction wrapping but still fire lifecycle events
        if (isCosmosDb()) {
            log.debugf("CosmosDB: @MorphiumTransactional on %s.%s executes WITHOUT transaction.",
                    ctx.getMethod().getDeclaringClass().getSimpleName(),
                    ctx.getMethod().getName());
            return proceedWithEvents(ctx);
        }

        // REQUIRED propagation: if a transaction is already active, just participate
        if (morphium.getTransaction() != null) {
            log.debugf("Joining existing transaction for %s.%s",
                    ctx.getMethod().getDeclaringClass().getSimpleName(),
                    ctx.getMethod().getName());
            return ctx.proceed();
        }

        try {
            morphium.startTransaction();
        } catch (UnsupportedOperationException e) {
            // Defensive fallback: detection missed CosmosDB (e.g. driver not yet connected at first check)
            cosmosDb = true;
            log.warn("startTransaction() threw UnsupportedOperationException — "
                    + "switching to CosmosDB mode for all future invocations.");
            return proceedWithEvents(ctx);
        }

        // Disable the write buffer for this thread while the transaction is active.
        // BufferedMorphiumWriter flushes on a background thread that does NOT
        // participate in the transaction — writes would bypass the transaction scope.
        // Save the current state so we only re-enable if it was enabled before,
        // avoiding clobbering a caller that had already disabled the write buffer.
        boolean writeBufferWasEnabled = morphium.isWriteBufferEnabledForThread();
        if (writeBufferWasEnabled) {
            morphium.disableWriteBufferForThread();
        }
        int maxRetries = 3;
        try {
            for (int attempt = 0; ; attempt++) {
                Object result;
                try {
                    result = ctx.proceed();
                } catch (Throwable t) {
                    // catch (Throwable), not (Exception): an Error (e.g. OutOfMemoryError,
                    // StackOverflowError) must still trigger safeAbort() -- otherwise the
                    // transaction context stays open on this thread, and a later invocation
                    // reusing the same (pooled) thread would silently "join" a dead
                    // transaction via the REQUIRED-propagation check above.
                    safeAbort();
                    if (!(t instanceof Exception e)) {
                        // Errors are never retried; rethrow as-is (no lifecycle event,
                        // matching how an unrecoverable JVM-level failure should propagate).
                        throw t;
                    }
                    if (attempt < maxRetries && isTransientTransactionError(e)) {
                        log.warnf("Transient transaction error on %s.%s (attempt %d/%d) — retrying entire transaction: %s",
                                ctx.getMethod().getDeclaringClass().getSimpleName(),
                                ctx.getMethod().getName(),
                                attempt + 1, maxRetries,
                                e.getMessage());
                        try {
                            long backoffMs = 50L * (1L << attempt); // exponential: 50, 100, 200ms
                            Thread.sleep(Math.min(backoffMs, 1000L));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            afterRollback.fire(new MorphiumTransactionEvent(Phase.AFTER_ROLLBACK, e));
                            throw e;
                        }
                        morphium.startTransaction();
                        continue;
                    }
                    afterRollback.fire(new MorphiumTransactionEvent(Phase.AFTER_ROLLBACK, e));
                    throw e;
                }

                // The business method itself succeeded -- from here on, a transient error
                // must retry ONLY the commit, never re-run ctx.proceed(). MongoDB drivers
                // retry a transient commit failure (e.g. code 251/NoSuchTransaction after a
                // failover where the server actually committed but the reply was lost) by
                // resending the commit, exactly for this reason: re-running the statements
                // that already ran inside the (possibly already-committed) transaction would
                // apply them a second time. safeCommitWithRetry() below handles that retry
                // internally and never re-invokes ctx.proceed().
                //
                // BEFORE_COMMIT below fires exactly once per successful ctx.proceed() -- it is
                // outside safeCommitWithRetry()'s own internal retry loop, so a transient commit
                // retry does NOT re-fire it (observers that key idempotency/outbox logic off this
                // event would otherwise see it multiple times for what is really the same logical
                // commit attempt). It only fires again if the OUTER loop above re-runs
                // ctx.proceed() from scratch, which is a genuinely new transaction attempt.
                try {
                    beforeCommit.fire(new MorphiumTransactionEvent(Phase.BEFORE_COMMIT));
                    safeCommitWithRetry(ctx);
                    afterCommit.fire(new MorphiumTransactionEvent(Phase.AFTER_COMMIT));
                    return result;
                } catch (Throwable t) {
                    safeAbort();
                    if (t instanceof Exception e) {
                        afterRollback.fire(new MorphiumTransactionEvent(Phase.AFTER_ROLLBACK, e));
                        throw e;
                    }
                    throw t;
                }
            }
        } finally {
            if (writeBufferWasEnabled) {
                morphium.enableWriteBufferForThread();
            }
        }
    }

    /**
     * Commits the current transaction, tolerating the case where no server-side
     * transaction exists (e.g. when all repository calls were mocked in tests
     * and no actual DB operations reached the server).
     */
    private void safeCommit() throws MorphiumDriverException {
        if (morphium.getTransaction() == null) {
            return;
        }
        try {
            morphium.commitTransaction();
        } catch (MorphiumDriverException e) {
            if (isNoServerTransaction(e)) {
                log.debugf("No server-side transaction to commit (no DB operations occurred): %s",
                        e.getMessage());
            } else {
                throw e;
            }
        }
    }

    /**
     * Commits the current transaction, retrying ONLY the commit itself (never
     * {@code ctx.proceed()}) up to {@code maxRetries} times when a transient MongoDB error
     * ({@link #isTransientTransactionError}) occurs. This mirrors how MongoDB drivers handle a
     * transient commit failure internally: a failed commit whose underlying write may have
     * actually succeeded on the server (the reply was merely lost, e.g. during a primary
     * failover -- code 251/NoSuchTransaction) is retried by resending the commit, never by
     * re-running the original statements. Re-running the whole {@code @MorphiumTransactional}
     * method here would apply every write inside it a second time.
     *
     * @param ctx the invocation context, used only for the log message's method name
     */
    private void safeCommitWithRetry(InvocationContext ctx) throws MorphiumDriverException {
        int maxRetries = 3;
        for (int attempt = 0; ; attempt++) {
            try {
                safeCommit();
                return;
            } catch (MorphiumDriverException e) {
                if (attempt >= maxRetries || !isTransientTransactionError(e)) {
                    throw e;
                }
                log.warnf("Transient error committing transaction for %s.%s (attempt %d/%d) — "
                                + "retrying the commit only, not the business method: %s",
                        ctx.getMethod().getDeclaringClass().getSimpleName(),
                        ctx.getMethod().getName(),
                        attempt + 1, maxRetries,
                        e.getMessage());
                long backoffMs = 50L * (1L << attempt); // exponential: 50, 100, 200ms
                try {
                    Thread.sleep(Math.min(backoffMs, 1000L));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    /**
     * Aborts the current transaction if one exists, tolerating the case where
     * no server-side transaction was started.
     */
    private void safeAbort() {
        if (morphium.getTransaction() == null) {
            return;
        }
        try {
            morphium.abortTransaction();
        } catch (MorphiumDriverException e) {
            if (isNoServerTransaction(e)) {
                log.debugf("No server-side transaction to abort (no DB operations occurred): %s",
                        e.getMessage());
            } else {
                log.warnf("Could not abort transaction: %s", e.getMessage());
            }
        } catch (Exception e) {
            log.warnf("Could not abort transaction: %s", e.getMessage());
        }
    }

    /**
     * Returns {@code true} if {@code e} indicates there was no server-side transaction to
     * commit/abort (e.g. every repository call inside the {@code @MorphiumTransactional} method
     * ran against a driver/collection that never actually reached the server, such as
     * {@code InMemDriver} in tests, or a method that made no writes at all).
     *
     * <p>This is a best-effort heuristic based on matching known MongoDB server error message
     * phrasings, not a documented MongoDB error code -- the driver layer (see
     * {@code PooledDriver.commitTransaction}/{@code abortTransaction}) throws a plain
     * {@code IllegalArgumentException} (not even a {@code MorphiumDriverException}) for the
     * "no transaction context on this driver" case, which {@link #safeCommit}/{@link #safeAbort}
     * already always short-circuit before reaching here via the {@code morphium.getTransaction()
     * == null} check. This method instead covers the server-side case: a transaction context
     * exists client-side, but the server never actually started a transaction for it (no
     * operation was sent under it) -- MongoDB itself rejects the commit/abort command in that
     * case, and the exact wording of that rejection is not part of any stable, code-based
     * contract we could match against instead of a string. If a future MongoDB server version
     * changes this wording, this check silently stops matching -- there is no more reliable
     * signal available to fall back to without a documented error code for this specific case.
     */
    static boolean isNoServerTransaction(MorphiumDriverException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("cannot start a transaction") || lower.contains("no transaction started")
                || lower.contains("no transaction is in progress") || lower.contains("no such transaction");
    }

    /**
     * Executes the intercepted method without transaction wrapping but fires
     * the same lifecycle events so that observers (outbox, cleanup, etc.) still work.
     */
    private Object proceedWithEvents(InvocationContext ctx) throws Throwable {
        try {
            Object result = ctx.proceed();
            beforeCommit.fire(new MorphiumTransactionEvent(Phase.BEFORE_COMMIT));
            afterCommit.fire(new MorphiumTransactionEvent(Phase.AFTER_COMMIT));
            return result;
        } catch (Throwable t) {
            if (t instanceof Exception e) {
                afterRollback.fire(new MorphiumTransactionEvent(Phase.AFTER_ROLLBACK, e));
            }
            throw t;
        }
    }

    /**
     * Returns {@code true} for a {@link CompletionStage} return type, or Mutiny's
     * {@code io.smallrye.mutiny.Uni} by class name (Mutiny is not a compile-time dependency of
     * this module, so it cannot be referenced directly — checking the name still correctly
     * detects it whether or not Mutiny happens to be on the runtime classpath).
     */
    static boolean isAsyncReturnType(Class<?> returnType) {
        return CompletionStage.class.isAssignableFrom(returnType)
                || "io.smallrye.mutiny.Uni".equals(returnType.getName());
    }

    /**
     * Returns {@code true} if the exception (or any cause in its chain) is a
     * transient MongoDB transaction error that is safe to retry:
     * <ul>
     *   <li>112 — WriteConflict (includes transaction eviction under load)</li>
     *   <li>251 — NoSuchTransaction (transaction expired on the server)</li>
     * </ul>
     */
    static boolean isTransientTransactionError(Exception e) {
        if (!(e instanceof MorphiumDriverException mde)) {
            // Check cause chain — Morphium exceptions are often wrapped
            Throwable cause = e.getCause();
            while (cause != null) {
                if (cause instanceof MorphiumDriverException mdeCause) {
                    return isTransientMongoCode(mdeCause);
                }
                cause = cause.getCause();
            }
            return false;
        }
        return isTransientMongoCode(mde);
    }

    private static boolean isTransientMongoCode(MorphiumDriverException e) {
        if (e.getMongoCode() instanceof Number mc) {
            int code = mc.intValue();
            return code == 112  // WriteConflict (incl. transaction eviction)
                || code == 251; // NoSuchTransaction
        }
        return false;
    }
}
