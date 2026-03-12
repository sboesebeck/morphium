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
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

@Path("/bank")
public class BankResource {

    @Inject
    Template bank;

    @Inject
    @Location("tags/learn-bank.html")
    Template learnBank;

    @Inject
    @Location("tags/demo-bank.html")
    Template demoBank;

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
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnBank.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        bankService.seedData();
        return demoData(null, null, null, null, null);
    }

    private TemplateInstance demoData(String success, String error, Object transferResult,
            String lastOperation, String lastMongoCommand) {
        return demoBank.data("accounts", bankService.findAllAccounts())
                .data("transfers", bankService.findAllTransfers())
                .data("accountCount", bankService.countAccounts())
                .data("transferResult", transferResult)
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    @POST
    @Path("/accounts")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createAccount(
            @FormParam("accountNumber") String accountNumber,
            @FormParam("ownerName") String ownerName,
            @FormParam("balance") double balance,
            @FormParam("currency") String currency,
            @Context HttpHeaders headers) {
        bankService.createAccount(accountNumber, ownerName, balance,
                currency != null && !currency.isBlank() ? currency : "EUR");
        if (isHtmx(headers)) return Response.ok(demoData("Account created.", null, null,
                "morphium.store(wallet)", "db.wallets.insertOne({...})")).build();
        return Response.seeOther(URI.create("/bank")).build();
    }

    @POST
    @Path("/transfer")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response transfer(
            @FormParam("from") String from,
            @FormParam("to") String to,
            @FormParam("amount") double amount,
            @FormParam("description") String description,
            @Context HttpHeaders headers) {
        var result = bankService.transfer(from, to, amount, description);
        if (isHtmx(headers)) {
            String msg = result.success() ? null : null;
            return Response.ok(demoData(null, null, result,
                    "@MorphiumTransactional: inc/dec", "startTransaction \u2192 updateOne \u00d7 2 \u2192 commitTransaction")).build();
        }
        return Response.ok(bank.data("active", "bank").data("docLinks", DOC_LINKS)).build();
    }

    @POST
    @Path("/deposit")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response deposit(
            @FormParam("accountNumber") String accountNumber,
            @FormParam("amount") double amount,
            @Context HttpHeaders headers) {
        bankService.deposit(accountNumber, amount);
        if (isHtmx(headers)) return Response.ok(demoData("Deposited " + amount + ".", null, null,
                "morphium.inc(query, \"balance\", amount)", "db.wallets.updateOne({_id: ...}, {$inc: {balance: N}})")).build();
        return Response.seeOther(URI.create("/bank")).build();
    }

    @POST
    @Path("/withdraw")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response withdraw(
            @FormParam("accountNumber") String accountNumber,
            @FormParam("amount") double amount,
            @Context HttpHeaders headers) {
        try {
            bankService.withdraw(accountNumber, amount);
            if (isHtmx(headers)) return Response.ok(demoData("Withdrawn " + amount + ".", null, null,
                    "morphium.inc(query, \"balance\", amount)", "db.wallets.updateOne({_id: ...}, {$inc: {balance: N}})")).build();
        } catch (IllegalArgumentException e) {
            if (isHtmx(headers)) return Response.ok(demoData(null, e.getMessage(), null,
                    "morphium.inc(query, \"balance\", amount)", "db.wallets.updateOne({_id: ...}, {$inc: {balance: N}})")).build();
        }
        return Response.seeOther(URI.create("/bank")).build();
    }

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed(@Context HttpHeaders headers) {
        bankService.deleteAll();
        bankService.seedData();
        if (isHtmx(headers)) return Response.ok(demoData("Sample data re-seeded.", null, null,
                "morphium.storeList(wallets)", "db.wallets.insertMany([...])")).build();
        return Response.seeOther(URI.create("/bank")).build();
    }
}
