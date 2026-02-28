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

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        bankService.deleteAll();
        bankService.seedData();
        return Response.seeOther(URI.create("/bank")).build();
    }
}