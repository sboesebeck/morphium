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
package de.caluga.morphium.showcase.bank;

import de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent;
import de.caluga.morphium.quarkus.transaction.MorphiumTxPhase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import static de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent.Phase.AFTER_COMMIT;
import static de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent.Phase.AFTER_ROLLBACK;
import static de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent.Phase.BEFORE_COMMIT;

/**
 * CDI observer that demonstrates the quarkus-morphium <strong>transaction lifecycle events</strong>
 * fired by {@code @MorphiumTransactional} (see
 * {@link de.caluga.morphium.quarkus.transaction.MorphiumTransactionalInterceptor}).
 *
 * <p>This is a feature distinct from {@code @MorphiumTransactional} itself: while the annotation
 * takes care of {@code startTransaction()} / {@code commitTransaction()} / {@code abortTransaction()},
 * the interceptor <em>also</em> fires CDI events at three points in that lifecycle, which any bean
 * can observe with {@code @Observes} + the {@link MorphiumTxPhase} qualifier:</p>
 *
 * <ul>
 *   <li><strong>{@code BEFORE_COMMIT}</strong> -- after the business method returned successfully,
 *       but before {@code morphium.commitTransaction()} is called.</li>
 *   <li><strong>{@code AFTER_COMMIT}</strong> -- after {@code commitTransaction()} succeeded.</li>
 *   <li><strong>{@code AFTER_ROLLBACK}</strong> -- after {@code abortTransaction()} was triggered by
 *       an exception. {@link MorphiumTransactionEvent#getFailure()} carries the causing exception.</li>
 * </ul>
 *
 * <p>Typical use cases for these observers: publishing an outbox/domain event only once a
 * transaction is durably committed, audit logging, cache invalidation, or alerting on rollbacks --
 * all decoupled from the transactional service method itself.</p>
 *
 * <p>This class keeps an in-memory ring buffer of the most recent events so the showcase UI
 * (see {@code BankService#getTransactionEvents()} and the Bank demo tab) can visualize them
 * without needing a real message broker.</p>
 */
@ApplicationScoped
public class TransactionLifecycleObserver {

    private static final Logger LOG = Logger.getLogger(TransactionLifecycleObserver.class);

    /** Bound on the in-memory event ring buffer used purely for the showcase UI. */
    private static final int MAX_EVENTS = 25;

    /**
     * A single recorded lifecycle event, simplified for display purposes.
     *
     * @param timestamp when the event was observed
     * @param phase     the {@link MorphiumTransactionEvent.Phase} name (BEFORE_COMMIT, AFTER_COMMIT,
     *                  AFTER_ROLLBACK)
     * @param detail    a human-readable description, including the failure cause for rollbacks
     */
    public record LifecycleLogEntry(LocalDateTime timestamp, String phase, String detail) {}

    private final Deque<LifecycleLogEntry> events = new ConcurrentLinkedDeque<>();

    /**
     * Observes {@code BEFORE_COMMIT}: fired once the {@code @MorphiumTransactional} business method
     * has returned normally, right before {@code morphium.commitTransaction()} runs.
     * {@code e.getFailure()} is always {@code null} here.
     */
    void onBeforeCommit(@Observes @MorphiumTxPhase(BEFORE_COMMIT) MorphiumTransactionEvent e) {
        LOG.debug("MorphiumTransactionEvent: BEFORE_COMMIT");
        record(e.getPhase(), "Business method succeeded; about to call morphium.commitTransaction()");
    }

    /**
     * Observes {@code AFTER_COMMIT}: fired once {@code morphium.commitTransaction()} has succeeded.
     * This is the safe point to trigger side effects that must only happen for durably persisted data
     * (e.g. publishing a domain event, invalidating a cache).
     */
    void onAfterCommit(@Observes @MorphiumTxPhase(AFTER_COMMIT) MorphiumTransactionEvent e) {
        LOG.debug("MorphiumTransactionEvent: AFTER_COMMIT");
        record(e.getPhase(), "Transaction committed successfully");
    }

    /**
     * Observes {@code AFTER_ROLLBACK}: fired after {@code morphium.abortTransaction()} was triggered
     * by an exception thrown from the business method. {@code e.getFailure()} carries the causing
     * exception -- the only phase where this is non-null.
     */
    void onAfterRollback(@Observes @MorphiumTxPhase(AFTER_ROLLBACK) MorphiumTransactionEvent e) {
        Exception failure = e.getFailure();
        String failureMessage = failure != null
                ? failure.getClass().getSimpleName() + ": " + failure.getMessage()
                : "unknown cause";
        LOG.warn("MorphiumTransactionEvent: AFTER_ROLLBACK -- transaction rolled back", failure);
        record(e.getPhase(), "Transaction aborted -- cause: " + failureMessage);
    }

    private void record(MorphiumTransactionEvent.Phase phase, String detail) {
        events.addFirst(new LifecycleLogEntry(LocalDateTime.now(), phase.name(), detail));
        while (events.size() > MAX_EVENTS) {
            events.removeLast();
        }
    }

    /**
     * Returns the most recently observed lifecycle events, newest first.
     *
     * @return an immutable snapshot of the in-memory event log (bounded to {@link #MAX_EVENTS})
     */
    public List<LifecycleLogEntry> recentEvents() {
        return List.copyOf(events);
    }

    /** Clears the in-memory event log. Used by the "Clear Log" showcase UI action. */
    public void clear() {
        events.clear();
    }
}
