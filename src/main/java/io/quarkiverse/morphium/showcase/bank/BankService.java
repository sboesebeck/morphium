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

@ApplicationScoped
public class BankService {

    @Inject
    Morphium morphium;

    public record TransferResult(boolean success, String message, Transfer transfer) {}

    public List<Account> findAllAccounts() {
        return morphium.createQueryFor(Account.class).asList();
    }

    public Account findAccountByNumber(String accountNumber) {
        return morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(accountNumber)
                .get();
    }

    public Account createAccount(String accountNumber, String ownerName, double initialBalance, String currency) {
        Account account = Account.builder()
                .accountNumber(accountNumber)
                .ownerName(ownerName)
                .balance(initialBalance)
                .currency(currency)
                .build();
        morphium.store(account);
        return account;
    }

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

        // Atomic decrement on source account
        var fromQuery = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(fromNumber);
        morphium.inc(fromQuery, Account.Fields.balance, -amount);

        // Atomic increment on target account
        var toQuery = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(toNumber);
        morphium.inc(toQuery, Account.Fields.balance, amount);

        // Record the transfer
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

    public void deposit(String accountNumber, double amount) {
        var query = morphium.createQueryFor(Account.class)
                .f(Account.Fields.accountNumber).eq(accountNumber);
        morphium.inc(query, Account.Fields.balance, amount);
    }

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
        morphium.inc(query, Account.Fields.balance, -amount);
    }

    public List<Transfer> findTransfers(String accountNumber) {
        // Find transfers where this account is either the source or the target
        var fromQuery = morphium.createQueryFor(Transfer.class)
                .f(Transfer.Fields.fromAccount).eq(accountNumber);
        var toQuery = morphium.createQueryFor(Transfer.class)
                .f(Transfer.Fields.toAccount).eq(accountNumber);

        List<Transfer> fromTransfers = fromQuery.sort(Map.of(Transfer.Fields.createdAt, -1)).asList();
        List<Transfer> toTransfers = toQuery.sort(Map.of(Transfer.Fields.createdAt, -1)).asList();

        // Merge and deduplicate by id
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

    public List<Transfer> findAllTransfers() {
        return morphium.createQueryFor(Transfer.class)
                .sort(Map.of(Transfer.Fields.createdAt, -1))
                .asList();
    }

    public void deleteAll() {
        morphium.dropCollection(Account.class);
        morphium.dropCollection(Transfer.class);
    }

    public long countAccounts() {
        return morphium.createQueryFor(Account.class).countAll();
    }

    public void seedData() {
        if (countAccounts() > 0) return;

        createAccount("ACC-001", "Alice", 1000.00, "EUR");
        createAccount("ACC-002", "Bob", 500.00, "EUR");
        createAccount("ACC-003", "Charlie", 250.00, "EUR");
    }
}