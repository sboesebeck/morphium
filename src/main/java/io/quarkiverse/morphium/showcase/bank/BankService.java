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
package io.quarkiverse.morphium.showcase.bank;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.quarkus.transaction.MorphiumTransactional;
import io.quarkiverse.morphium.showcase.bank.entity.Account;
import io.quarkiverse.morphium.showcase.bank.entity.Transfer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 * Service layer demonstrating Morphium's transactional support, atomic field operations, and
 * optimistic locking in a banking scenario.
 *
 * <p>Key Morphium features demonstrated:</p>
 * <ul>
 *   <li><strong>{@code @MorphiumTransactional}</strong> -- Declarative MongoDB transactions. Wraps the
 *       annotated method in a MongoDB multi-document transaction (requires a replica set). If any
 *       exception occurs, the entire transaction is rolled back. This is the Morphium equivalent of
 *       Spring's {@code @Transactional}.</li>
 *   <li><strong>{@code morphium.inc()}</strong> -- Atomic field increment/decrement. This maps directly
 *       to MongoDB's {@code $inc} update operator and is executed atomically on the server. It avoids
 *       the classic read-modify-write race condition because the operation is performed in a single
 *       database round-trip.</li>
 *   <li><strong>{@code @Version} (on Account)</strong> -- Optimistic locking that prevents lost updates
 *       when multiple clients modify the same document concurrently.</li>
 * </ul>
 *
 * <h3>Why use {@code morphium.inc()} instead of read-modify-write?</h3>
 * <p>Consider this naive approach:</p>
 * <pre>{@code
 *   Account acc = findByNumber("ACC-001");  // balance = 100
 *   acc.setBalance(acc.getBalance() + 50);  // balance = 150 in Java
 *   morphium.store(acc);                    // writes 150 to DB
 * }</pre>
 * <p>If two threads do this concurrently, both read 100, both write 150, and you lose one deposit.
 * With {@code morphium.inc(query, "balance", 50)}, MongoDB atomically adds 50 to whatever the
 * current value is, so concurrent operations are safe.</p>
 */
@ApplicationScoped
public class BankService {

    @Inject
    Morphium morphium;

    /**
     * Result record for transfer operations, containing success status, a message, and the
     * transfer entity (if successful).
     */
    public record TransferResult(boolean success, String message, Transfer transfer) {}

    /**
     * Retrieves all bank accounts.
     *
     * @return a list of all accounts in the "accounts" collection
     */
    public List<Account> findAllAccounts() {
        // createQueryFor() without any filter returns all documents in the collection.
        return morphium.createQueryFor(Account.class).asList();
    }

    /**
     * Finds an account by its unique account number.
     *
     * @param accountNumber the account number to look up (e.g., "ACC-001")
     * @return the matching account, or {@code null} if not found
     */
    public Account findAccountByNumber(String accountNumber) {
        // f() selects the field to filter on; eq() adds an equality condition.
        // get() returns a single result (the first match) or null if no document matches.
        return morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(accountNumber)
                .get();
    }

    /**
     * Creates a new bank account and persists it.
     *
     * <p>On first store, Morphium will:</p>
     * <ul>
     *   <li>Generate a new {@code MorphiumId} for the {@code @Id} field</li>
     *   <li>Set {@code @CreationTime} to the current timestamp</li>
     *   <li>Initialize {@code @Version} to 1</li>
     * </ul>
     *
     * @param accountNumber  the unique account number
     * @param ownerName      the account owner's name
     * @param initialBalance the starting balance
     * @param currency       the currency code (e.g., "EUR")
     * @return the newly created account with all auto-populated fields
     */
    public Account createAccount(String accountNumber, String ownerName, double initialBalance, String currency) {
        Account account = Account.builder()
                .accountNumber(accountNumber)
                .ownerName(ownerName)
                .balance(initialBalance)
                .currency(currency)
                .build();
        // store() inserts the new account. Morphium auto-populates @Id, @Version, and @CreationTime.
        morphium.store(account);
        return account;
    }

    /**
     * Transfers money between two accounts within a MongoDB transaction.
     *
     * <p>{@code @MorphiumTransactional} wraps this entire method in a MongoDB multi-document
     * transaction. If any operation fails or an exception is thrown, ALL changes (both balance
     * updates and the transfer record) are rolled back atomically. This ensures that money is
     * never "lost" due to a partial failure.</p>
     *
     * <p><strong>Important:</strong> MongoDB transactions require a replica set (or sharded cluster).
     * They do NOT work on standalone MongoDB instances. The Quarkus dev setup uses a single-node
     * replica set to support transactions in development.</p>
     *
     * @param fromNumber  the source account number
     * @param toNumber    the target account number
     * @param amount      the amount to transfer (must be positive)
     * @param description a human-readable description of the transfer
     * @return a {@link TransferResult} indicating success or failure with a message
     */
    // @MorphiumTransactional: Starts a MongoDB session + transaction before this method executes.
    // On normal return, the transaction is committed. On exception, it is aborted (rolled back).
    @MorphiumTransactional
    public TransferResult transfer(String fromNumber, String toNumber, double amount, String description) {
        Account fromAccount = findAccountByNumber(fromNumber);
        Account toAccount = findAccountByNumber(toNumber);

        if (fromAccount == null) {
            return new TransferResult(false, "Source account not found: " + fromNumber, null);
        }
        if (toAccount == null) {
            return new TransferResult(false, "Target account not found: " + toNumber, null);
        }
        if (fromAccount.getBalance() < amount) {
            return new TransferResult(false,
                    "Insufficient funds. Available: " + fromAccount.getBalance() + ", requested: " + amount, null);
        }
        if (amount <= 0) {
            return new TransferResult(false, "Transfer amount must be positive", null);
        }

        // morphium.inc() performs an atomic $inc update on the matched document(s).
        // Using a negative amount for decrement avoids race conditions -- the subtraction
        // happens server-side in a single atomic operation, not in Java memory.
        var fromQuery = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(fromNumber);
        morphium.inc(fromQuery, Account.Fields.balance, -amount);

        // Atomic increment on the target account. MongoDB's $inc guarantees that concurrent
        // deposits/transfers won't interfere with each other.
        var toQuery = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(toNumber);
        morphium.inc(toQuery, Account.Fields.balance, amount);

        // Record the transfer as an audit log entry.
        // This store() participates in the same transaction as the balance updates above.
        Transfer transfer = Transfer.builder()
                .fromAccount(fromNumber)
                .toAccount(toNumber)
                .amount(amount)
                .currency(fromAccount.getCurrency())
                .description(description)
                .status("COMPLETED")
                .build();
        morphium.store(transfer);

        return new TransferResult(true,
                "Transferred " + amount + " " + fromAccount.getCurrency() + " from " + fromNumber + " to " + toNumber,
                transfer);
    }

    /**
     * Deposits money into an account using an atomic increment.
     *
     * <p>Uses {@code morphium.inc()} with a positive amount to atomically add to the balance.
     * This is safe for concurrent deposits without requiring a transaction or optimistic locking.</p>
     *
     * @param accountNumber the account to deposit into
     * @param amount        the amount to deposit (must be positive)
     */
    public void deposit(String accountNumber, double amount) {
        var query = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(accountNumber);
        // inc() with a positive value atomically adds to the balance field.
        // The query selects which document(s) to update; the field and amount specify the $inc operation.
        morphium.inc(query, Account.Fields.balance, amount);
    }

    /**
     * Withdraws money from an account after checking for sufficient funds.
     *
     * <p>Note: The balance check and the decrement are NOT in a single atomic operation here.
     * In a production system, you would either use {@code @MorphiumTransactional} or perform
     * a conditional update (findAndModify with balance >= amount) to prevent overdrafts under
     * concurrent access.</p>
     *
     * @param accountNumber the account to withdraw from
     * @param amount        the amount to withdraw
     * @throws IllegalArgumentException if the account is not found or has insufficient funds
     */
    public void withdraw(String accountNumber, double amount) {
        Account account = findAccountByNumber(accountNumber);
        if (account == null) {
            throw new IllegalArgumentException("Account not found: " + accountNumber);
        }
        if (account.getBalance() < amount) {
            throw new IllegalArgumentException(
                    "Insufficient funds. Available: " + account.getBalance() + ", requested: " + amount);
        }
        var query = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(accountNumber);
        // inc() with a negative value atomically decrements the balance.
        morphium.inc(query, Account.Fields.balance, -amount);
    }

    /**
     * Finds all transfers involving a specific account (as sender or recipient).
     *
     * <p>Since Morphium's query API does not have a built-in OR operator at the top level for
     * different fields, this method executes two separate queries and merges the results in Java.
     * An alternative would be to use MongoDB's {@code $or} operator via Morphium's
     * {@code or()} query method.</p>
     *
     * @param accountNumber the account number to find transfers for
     * @return transfers involving this account, sorted by creation time descending (newest first)
     */
    public List<Transfer> findTransfers(String accountNumber) {
        // Query 1: Find transfers where this account is the sender
        var fromQuery = morphium.createQueryFor(Transfer.class)
                .f(Transfer.Fields.fromAccount).eq(accountNumber);
        // Query 2: Find transfers where this account is the recipient
        var toQuery = morphium.createQueryFor(Transfer.class)
                .f(Transfer.Fields.toAccount).eq(accountNumber);

        // sort() with -1 means descending order (newest first).
        List<Transfer> fromTransfers = fromQuery.sort(Map.of(Transfer.Fields.createdAt, -1)).asList();
        List<Transfer> toTransfers = toQuery.sort(Map.of(Transfer.Fields.createdAt, -1)).asList();

        // Merge and deduplicate by id (a transfer from A to B appears in both queries)
        java.util.LinkedHashMap<MorphiumId, Transfer> merged = new java.util.LinkedHashMap<>();
        fromTransfers.forEach(t -> merged.put(t.getId(), t));
        toTransfers.forEach(t -> merged.put(t.getId(), t));

        return merged.values().stream()
                .sorted((a, b) -> {
                    if (a.getCreatedAt() == null || b.getCreatedAt() == null) return 0;
                    return b.getCreatedAt().compareTo(a.getCreatedAt());
                })
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Retrieves all transfers, sorted by creation time descending.
     *
     * @return all transfer records, newest first
     */
    public List<Transfer> findAllTransfers() {
        return morphium.createQueryFor(Transfer.class)
                .sort(Map.of(Transfer.Fields.createdAt, -1))
                .asList();
    }

    /**
     * Drops both the "accounts" and "transfers" collections from MongoDB.
     * Used for test setup and the "reset data" UI action.
     */
    public void deleteAll() {
        // dropCollection() removes the entire collection including all indexes.
        morphium.dropCollection(Account.class);
        morphium.dropCollection(Transfer.class);
    }

    /**
     * Counts the total number of accounts.
     *
     * @return the account count
     */
    public long countAccounts() {
        return morphium.createQueryFor(Account.class).countAll();
    }

    /**
     * Seeds sample account data if the collection is empty.
     * Creates three test accounts with different balances.
     */
    public void seedData() {
        if (countAccounts() > 0) return;

        createAccount("ACC-001", "Alice", 1000.00, "EUR");
        createAccount("ACC-002", "Bob", 500.00, "EUR");
        createAccount("ACC-003", "Charlie", 250.00, "EUR");
    }
}