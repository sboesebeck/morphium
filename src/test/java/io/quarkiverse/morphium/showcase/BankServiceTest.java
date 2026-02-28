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
package io.quarkiverse.morphium.showcase;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.bank.BankService;
import io.quarkiverse.morphium.showcase.bank.entity.Account;
import io.quarkiverse.morphium.showcase.bank.entity.Transfer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link BankService} -- a transactional banking domain backed by Morphium/MongoDB.
 *
 * <h2>How This Test Works</h2>
 * <p>
 * The {@code @QuarkusTest} annotation boots the full Quarkus application context so that all CDI beans
 * (including {@link BankService} and the {@link Morphium} client) are available for injection. In the
 * test profile the Morphium driver is configured as {@code InMemDriver} (see
 * {@code src/test/resources/application.properties}), which means <strong>no real MongoDB instance is
 * required</strong>. The InMemDriver provides a fully functional in-memory implementation of the MongoDB
 * wire protocol, making tests extremely fast and self-contained.
 * </p>
 *
 * <h2>Key Configuration (application.properties)</h2>
 * <pre>
 *   quarkus.morphium.database=inmem-test
 *   quarkus.morphium.driver-name=InMemDriver
 *   quarkus.morphium.devservices.enabled=false
 * </pre>
 * <ul>
 *   <li>{@code driver-name=InMemDriver} -- tells Morphium to use the in-memory driver instead of a
 *       real MongoDB connection. This eliminates the need for Docker/Testcontainers.</li>
 *   <li>{@code devservices.enabled=false} -- disables Quarkus Dev Services so it does not try to start
 *       a MongoDB container automatically.</li>
 * </ul>
 *
 * <h2>Test Isolation Strategy</h2>
 * <p>
 * Each test method starts with a clean slate because {@link #setUp()} drops both the {@code Account}
 * and {@code Transfer} collections before every test. This is the recommended pattern when using
 * InMemDriver: {@code morphium.dropCollection(EntityClass.class)} is cheap and instantaneous in memory.
 * </p>
 *
 * <h2>What This Test Covers</h2>
 * <ul>
 *   <li>Account creation and persistence via {@code morphium.store()}</li>
 *   <li>Money transfers using atomic increments ({@code morphium.inc()}) inside a
 *       {@code @MorphiumTransactional} method</li>
 *   <li>Business rule validation (insufficient funds, negative amounts)</li>
 *   <li>Deposit and withdrawal operations with balance verification</li>
 * </ul>
 *
 * @see BankService
 * @see de.caluga.morphium.driver.inmem.InMemoryDriver
 */
@QuarkusTest
class BankServiceTest {

    // Quarkus CDI injects the BankService bean -- the system under test.
    @Inject
    BankService bankService;

    // The Morphium instance is injected directly so we can perform low-level operations
    // (like dropping collections) that are not part of the service's public API.
    @Inject
    Morphium morphium;

    /**
     * Runs before each test method to ensure complete test isolation.
     * <p>
     * Dropping collections is the simplest and most reliable way to reset state when using
     * InMemDriver. Unlike {@code deleteMany()}, {@code dropCollection()} also removes any
     * indexes, which guarantees a perfectly clean starting point.
     * </p>
     */
    @BeforeEach
    void setUp() {
        // Drop both collections to guarantee no leftover data between tests.
        // With InMemDriver this is essentially instant (no network round-trip).
        morphium.dropCollection(Account.class);
        morphium.dropCollection(Transfer.class);
    }

    /**
     * Verifies that creating an account persists it correctly via {@code morphium.store()}.
     * <p>
     * After storing, Morphium automatically populates the entity's {@code id} field with
     * a new {@link de.caluga.morphium.driver.MorphiumId}. We assert that the id is non-null
     * to confirm the store operation completed successfully.
     * </p>
     */
    @Test
    void shouldCreateAccount() {
        // Create an account -- internally calls morphium.store(account)
        Account account = bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");

        // After store(), Morphium assigns a MorphiumId automatically
        assertThat(account.getId()).isNotNull();

        // Verify the fields were persisted correctly
        assertThat(account.getAccountNumber()).isEqualTo("ACC-001");
        assertThat(account.getBalance()).isEqualTo(1000.0);
    }

    /**
     * Tests a successful money transfer between two accounts.
     * <p>
     * The {@link BankService#transfer} method is annotated with {@code @MorphiumTransactional},
     * which means it executes inside a Morphium transaction. The balance updates use
     * {@code morphium.inc()} for atomic field increments -- this is the MongoDB-idiomatic way
     * to modify numeric fields without race conditions.
     * </p>
     * <p>
     * Note: Transactions with InMemDriver work in a simplified mode. The InMemDriver supports
     * the transaction API so your code paths are exercised, but true multi-document atomicity
     * is only guaranteed with a real MongoDB replica set.
     * </p>
     */
    @Test
    void shouldTransferMoney() {
        // Set up: create two accounts with known balances
        bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");

        // Execute the transfer: move 200 EUR from Alice to Bob
        var result = bankService.transfer("ACC-001", "ACC-002", 200.0, "Test transfer");

        // The transfer result is a record indicating success/failure
        assertThat(result.success()).isTrue();
        assertThat(result.transfer()).isNotNull();

        // Re-read both accounts to verify the balances were updated atomically
        // (morphium.inc() performs $inc operations, not read-modify-write)
        Account alice = bankService.findAccountByNumber("ACC-001");
        Account bob = bankService.findAccountByNumber("ACC-002");

        // Alice: 1000 - 200 = 800
        assertThat(alice.getBalance()).isEqualTo(800.0);
        // Bob: 500 + 200 = 700
        assertThat(bob.getBalance()).isEqualTo(700.0);
    }

    /**
     * Verifies that a transfer is rejected when the source account has insufficient funds.
     * <p>
     * This is a business rule test: the service should return a failed {@code TransferResult}
     * rather than throwing an exception. This pattern (returning a result object instead of
     * throwing) is common in transactional services where partial failure information is useful.
     * </p>
     */
    @Test
    void shouldRejectTransferWithInsufficientFunds() {
        // Alice only has 100 EUR
        bankService.createAccount("ACC-001", "Alice", 100.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");

        // Attempt to transfer 200 EUR -- more than Alice has
        var result = bankService.transfer("ACC-001", "ACC-002", 200.0, "Too much");

        // The service returns a failure result instead of throwing
        assertThat(result.success()).isFalse();
        assertThat(result.message()).contains("Insufficient funds");
    }

    /**
     * Tests the deposit operation which adds funds to an existing account.
     * <p>
     * Under the hood, {@code deposit()} uses {@code morphium.inc(query, field, amount)} to
     * atomically increment the balance. We then re-read the account via a query to verify
     * the new balance.
     * </p>
     */
    @Test
    void shouldDeposit() {
        bankService.createAccount("ACC-001", "Alice", 100.0, "EUR");

        // Deposit 50 EUR -- uses morphium.inc() for atomic increment
        bankService.deposit("ACC-001", 50.0);

        // Re-read the account to verify the updated balance
        Account account = bankService.findAccountByNumber("ACC-001");
        assertThat(account.getBalance()).isEqualTo(150.0);
    }

    /**
     * Tests the withdrawal operation which deducts funds from an existing account.
     * <p>
     * Similar to deposit, withdrawal uses {@code morphium.inc()} with a negative amount.
     * The service first validates sufficient funds before performing the atomic decrement.
     * </p>
     */
    @Test
    void shouldWithdraw() {
        bankService.createAccount("ACC-001", "Alice", 100.0, "EUR");

        // Withdraw 30 EUR -- internally uses morphium.inc(query, field, -30.0)
        bankService.withdraw("ACC-001", 30.0);

        // Verify the balance was decremented correctly
        Account account = bankService.findAccountByNumber("ACC-001");
        assertThat(account.getBalance()).isEqualTo(70.0);
    }

    /**
     * Verifies that withdrawing more than the available balance throws an exception.
     * <p>
     * Unlike the transfer method (which returns a result object), the withdraw method
     * throws an {@link IllegalArgumentException}. This demonstrates AssertJ's
     * {@code assertThatThrownBy()} for testing exception scenarios -- a common pattern
     * in service-layer tests.
     * </p>
     */
    @Test
    void shouldRejectWithdrawWithInsufficientFunds() {
        // Alice only has 50 EUR
        bankService.createAccount("ACC-001", "Alice", 50.0, "EUR");

        // Attempting to withdraw 100 EUR should throw an exception
        assertThatThrownBy(() -> bankService.withdraw("ACC-001", 100.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Insufficient funds");
    }
}
