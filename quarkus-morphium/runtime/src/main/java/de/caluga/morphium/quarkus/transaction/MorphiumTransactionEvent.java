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

/**
 * CDI event fired by {@link MorphiumTransactionalInterceptor} at various
 * transaction lifecycle phases.
 */
public class MorphiumTransactionEvent {

    public enum Phase {
        BEFORE_COMMIT,
        AFTER_COMMIT,
        AFTER_ROLLBACK
    }

    private final Phase phase;
    private final Exception failure;

    public MorphiumTransactionEvent(Phase phase) {
        this(phase, null);
    }

    public MorphiumTransactionEvent(Phase phase, Exception failure) {
        this.phase = phase;
        this.failure = failure;
    }

    public Phase getPhase() {
        return phase;
    }

    /** Non-null only for {@link Phase#AFTER_ROLLBACK}. */
    public Exception getFailure() {
        return failure;
    }
}
