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

@QuarkusTest
class BankServiceTest {

    @Inject
    BankService bankService;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(Account.class);
        morphium.dropCollection(Transfer.class);
    }

    @Test
    void shouldCreateAccount() {
        Account account = bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");
        assertThat(account.getId()).isNotNull();
        assertThat(account.getAccountNumber()).isEqualTo("ACC-001");
        assertThat(account.getBalance()).isEqualTo(1000.0);
    }

    @Test
    void shouldTransferMoney() {
        bankService.createAccount("ACC-001", "Alice", 1000.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");

        var result = bankService.transfer("ACC-001", "ACC-002", 200.0, "Test transfer");

        assertThat(result.success()).isTrue();
        assertThat(result.transfer()).isNotNull();

        Account alice = bankService.findAccountByNumber("ACC-001");
        Account bob = bankService.findAccountByNumber("ACC-002");

        assertThat(alice.getBalance()).isEqualTo(800.0);
        assertThat(bob.getBalance()).isEqualTo(700.0);
    }

    @Test
    void shouldRejectTransferWithInsufficientFunds() {
        bankService.createAccount("ACC-001", "Alice", 100.0, "EUR");
        bankService.createAccount("ACC-002", "Bob", 500.0, "EUR");

        var result = bankService.transfer("ACC-001", "ACC-002", 200.0, "Too much");

        assertThat(result.success()).isFalse();
        assertThat(result.message()).contains("Insufficient funds");
    }

    @Test
    void shouldDeposit() {
        bankService.createAccount("ACC-001", "Alice", 100.0, "EUR");
        bankService.deposit("ACC-001", 50.0);

        Account account = bankService.findAccountByNumber("ACC-001");
        assertThat(account.getBalance()).isEqualTo(150.0);
    }

    @Test
    void shouldWithdraw() {
        bankService.createAccount("ACC-001", "Alice", 100.0, "EUR");
        bankService.withdraw("ACC-001", 30.0);

        Account account = bankService.findAccountByNumber("ACC-001");
        assertThat(account.getBalance()).isEqualTo(70.0);
    }

    @Test
    void shouldRejectWithdrawWithInsufficientFunds() {
        bankService.createAccount("ACC-001", "Alice", 50.0, "EUR");

        assertThatThrownBy(() -> bankService.withdraw("ACC-001", 100.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Insufficient funds");
    }
}