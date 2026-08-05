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
    Object aroundInvoke(InvocationContext ctx) throws Exception {
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
                try {
                    Object result = ctx.proceed();
                    beforeCommit.fire(new MorphiumTransactionEvent(Phase.BEFORE_COMMIT));
                    safeCommit();
                    afterCommit.fire(new MorphiumTransactionEvent(Phase.AFTER_COMMIT));
                    return result;
                } catch (Exception e) {
                    safeAbort();
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

    private static boolean isNoServerTransaction(MorphiumDriverException e) {
        String msg = e.getMessage();
        return msg != null && msg.contains("Cannot start a transaction");
    }

    /**
     * Executes the intercepted method without transaction wrapping but fires
     * the same lifecycle events so that observers (outbox, cleanup, etc.) still work.
     */
    private Object proceedWithEvents(InvocationContext ctx) throws Exception {
        try {
            Object result = ctx.proceed();
            beforeCommit.fire(new MorphiumTransactionEvent(Phase.BEFORE_COMMIT));
            afterCommit.fire(new MorphiumTransactionEvent(Phase.AFTER_COMMIT));
            return result;
        } catch (Exception e) {
            afterRollback.fire(new MorphiumTransactionEvent(Phase.AFTER_ROLLBACK, e));
            throw e;
        }
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
