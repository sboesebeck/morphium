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
package de.caluga.morphium.showcase;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.showcase.bank.BankService;
import de.caluga.morphium.showcase.bank.entity.Account;
import de.caluga.morphium.showcase.bank.entity.Transfer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for the {@code @MorphiumTransactional} CDI transaction-lifecycle events
 * (BEFORE_COMMIT, AFTER_COMMIT, AFTER_ROLLBACK) demonstrated via
 * {@link de.caluga.morphium.showcase.bank.TransactionLifecycleObserver}.
 *
 * <p>These events are fired by
 * {@code de.caluga.morphium.quarkus.transaction.MorphiumTransactionalInterceptor} and observed
 * with {@code @Observes @MorphiumTxPhase(...)}. This test drives {@link BankService#transferWithLifecycleDemo}
 * to trigger both the success path (BEFORE_COMMIT + AFTER_COMMIT) and the forced-failure path
 * (AFTER_ROLLBACK), then asserts on the observer's recorded event log.</p>
 *
 * @see de.caluga.morphium.showcase.bank.TransactionLifecycleObserver
 * @see BankService#transferWithLifecycleDemo
 */
@QuarkusTest
class TransactionLifecycleObserverTest {

    @Inject
    BankService bankService;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(Account.class);
        morphium.dropCollection(Transfer.class);
        bankService.clearTransactionLifecycleEvents();
    }

    /**
     * A successful transactional call must fire BEFORE_COMMIT then AFTER_COMMIT, and must NOT
     * fire AFTER_ROLLBACK.
     */
    @Test
    void shouldFireBeforeAndAfterCommitOnSuccess() {
        bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");

        var result = bankService.transferWithLifecycleDemo("ACC-001", "ACC-002", 100.0, "test", false);
        assertThat(result.success()).isTrue();

        var events = bankService.getTransactionLifecycleEvents();
        // recorded newest-first: AFTER_COMMIT then BEFORE_COMMIT
        assertThat(events).extracting("phase").containsExactly("AFTER_COMMIT", "BEFORE_COMMIT");
        assertThat(events).noneMatch(e -> e.phase().equals("AFTER_ROLLBACK"));
    }

    /**
     * When the business method throws before performing any write, the transaction is aborted
     * and AFTER_ROLLBACK fires -- BEFORE_COMMIT/AFTER_COMMIT must NOT fire.
     */
    @Test
    void shouldFireAfterRollbackOnForcedFailure() {
        bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");

        assertThatThrownBy(() ->
                bankService.transferWithLifecycleDemo("ACC-001", "ACC-002", 100.0, "test", true))
                .isInstanceOf(IllegalStateException.class);

        var events = bankService.getTransactionLifecycleEvents();
        assertThat(events).extracting("phase").containsExactly("AFTER_ROLLBACK");
        assertThat(events).noneMatch(e -> e.phase().equals("BEFORE_COMMIT"));
        assertThat(events).noneMatch(e -> e.phase().equals("AFTER_COMMIT"));

        // no funds were moved since the exception fired before any write
        Account alice = bankService.findAccountByNumber("ACC-001");
        Account bob = bankService.findAccountByNumber("ACC-002");
        assertThat(alice.getBalance()).isEqualTo(1000.0);
        assertThat(bob.getBalance()).isEqualTo(500.0);
    }

    /** Verifies the "Clear Log" showcase action empties the in-memory event log. */
    @Test
    void shouldClearEventLog() {
        bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");
        bankService.transferWithLifecycleDemo("ACC-001", "ACC-002", 50.0, "test", false);

        assertThat(bankService.getTransactionLifecycleEvents()).isNotEmpty();

        bankService.clearTransactionLifecycleEvents();

        assertThat(bankService.getTransactionLifecycleEvents()).isEmpty();
    }
}
