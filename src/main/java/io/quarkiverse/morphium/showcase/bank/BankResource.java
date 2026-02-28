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

import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

/**
 * JAX-RS resource serving the banking demo pages.
 *
 * <p>This resource demonstrates how Morphium handles transactional operations, atomic field updates,
 * and optimistic locking through the {@link BankService}. Key Morphium features exposed through
 * this UI:</p>
 * <ul>
 *   <li><strong>Account creation</strong> -- {@code morphium.store()} with auto-populated @Version,
 *       @CreationTime, and @Id fields</li>
 *   <li><strong>Money transfers</strong> -- {@code @MorphiumTransactional} wrapping multiple atomic
 *       {@code morphium.inc()} operations</li>
 *   <li><strong>Deposits/Withdrawals</strong> -- Atomic {@code morphium.inc()} without transactions</li>
 * </ul>
 */
@Path("/bank")
public class BankResource {

    @Inject
    Template bank;

    @Inject
    BankService bankService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/howtos/optimistic-locking", "Optimistic Locking", "@Version, Conflict Detection"),
            new DocLink("/docs/developer-guide", "Developer Guide", "Transactions, @MorphiumTransactional"),
            new DocLink("/docs/api-reference", "API Reference", "inc(), dec(), Atomic Operations")
    );

    /**
     * Renders the main banking page showing all accounts and recent transfers.
     * Seeds sample data on first access if the collection is empty.
     *
     * @return the rendered bank HTML page
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        bankService.seedData();
        return bank.data("active", "bank")
                .data("accounts", bankService.findAllAccounts())
                .data("transfers", bankService.findAllTransfers())
                .data("accountCount", bankService.countAccounts())
                .data("transferResult", null)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Creates a new bank account. Demonstrates Morphium's {@code store()} with automatic
     * {@code @Version}, {@code @CreationTime}, and {@code @Id} population.
     *
     * @param accountNumber  the unique account identifier
     * @param ownerName      the account holder's name
     * @param initialBalance the starting balance
     * @param currency       the currency code (defaults to "EUR")
     * @return a redirect to the bank page
     */
    @POST
    @Path("/accounts")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createAccount(
            @FormParam("accountNumber") String accountNumber,
            @FormParam("ownerName") String ownerName,
            @FormParam("initialBalance") double initialBalance,
            @FormParam("currency") String currency) {
        bankService.createAccount(accountNumber, ownerName, initialBalance,
                currency != null && !currency.isBlank() ? currency : "EUR");
        return Response.seeOther(URI.create("/bank")).build();
    }

    /**
     * Executes a money transfer between two accounts. This endpoint triggers the
     * {@code @MorphiumTransactional} transfer method, which wraps all operations in a
     * MongoDB multi-document transaction.
     *
     * @param from        the source account number
     * @param to          the target account number
     * @param amount      the transfer amount
     * @param description a human-readable description
     * @return the bank page with the transfer result (success or error message)
     */
    @POST
    @Path("/transfer")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance transfer(
            @FormParam("from") String from,
            @FormParam("to") String to,
            @FormParam("amount") double amount,
            @FormParam("description") String description) {
        var result = bankService.transfer(from, to, amount, description);
        return bank.data("active", "bank")
                .data("accounts", bankService.findAllAccounts())
                .data("transfers", bankService.findAllTransfers())
                .data("accountCount", bankService.countAccounts())
                .data("transferResult", result)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Deposits money into an account using Morphium's atomic {@code inc()} operation.
     *
     * @param accountNumber the account to deposit into
     * @param amount        the deposit amount
     * @return a redirect to the bank page
     */
    @POST
    @Path("/deposit")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response deposit(
            @FormParam("accountNumber") String accountNumber,
            @FormParam("amount") double amount) {
        bankService.deposit(accountNumber, amount);
        return Response.seeOther(URI.create("/bank")).build();
    }

    /**
     * Withdraws money from an account. Uses Morphium's atomic {@code inc()} with a negative value
     * after validating sufficient funds.
     *
     * @param accountNumber the account to withdraw from
     * @param amount        the withdrawal amount
     * @return a redirect to the bank page
     */
    @POST
    @Path("/withdraw")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response withdraw(
            @FormParam("accountNumber") String accountNumber,
            @FormParam("amount") double amount) {
        try {
            bankService.withdraw(accountNumber, amount);
        } catch (IllegalArgumentException e) {
            // Redirect back with error shown on next page load
            return Response.seeOther(URI.create("/bank")).build();
        }
        return Response.seeOther(URI.create("/bank")).build();
    }

    /**
     * Resets the banking data by dropping both collections and re-seeding sample accounts.
     *
     * @return a redirect to the bank page
     */
    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        bankService.deleteAll();
        bankService.seedData();
        return Response.seeOther(URI.create("/bank")).build();
    }
}