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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent;
import de.caluga.morphium.quarkus.transaction.MorphiumTxPhase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent.Phase.*;

/**
 * Collects {@link MorphiumTransactionEvent}s for test assertions.
 */
@ApplicationScoped
public class TransactionEventCollector {

    private final List<MorphiumTransactionEvent> events = new CopyOnWriteArrayList<>();

    void onBeforeCommit(@Observes @MorphiumTxPhase(BEFORE_COMMIT) MorphiumTransactionEvent e) {
        events.add(e);
    }

    void onAfterCommit(@Observes @MorphiumTxPhase(AFTER_COMMIT) MorphiumTransactionEvent e) {
        events.add(e);
    }

    void onAfterRollback(@Observes @MorphiumTxPhase(AFTER_ROLLBACK) MorphiumTransactionEvent e) {
        events.add(e);
    }

    public List<MorphiumTransactionEvent> getEvents() {
        return events;
    }

    public void clear() {
        events.clear();
    }
}
