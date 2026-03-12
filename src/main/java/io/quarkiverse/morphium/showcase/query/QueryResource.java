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
package io.quarkiverse.morphium.showcase.query;

import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
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

@Path("/query")
public class QueryResource {

    @Inject
    Template query;

    @Inject
    @Location("tags/learn-query.html")
    Template learnQuery;

    @Inject
    @Location("tags/demo-query.html")
    Template demoQuery;

    @Inject
    QueryShowcaseService queryShowcaseService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/api-reference", "API Reference", "Query API: eq, gte, lte, matches, in, nin"),
            new DocLink("/docs/developer-guide", "Developer Guide", "@Aliases, @Transient, @FieldNameConstants"),
            new DocLink("/docs/howtos/field-names", "Field Names", "Type-safe Field References")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        queryShowcaseService.seedData();
        return query.data("active", "query")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnQuery.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        queryShowcaseService.seedData();
        return demoAll(null, null, null, null);
    }

    private TemplateInstance demoAll(String success, String error,
            String lastOperation, String lastMongoCommand) {
        return demoQuery.data("employees", queryShowcaseService.findAll())
                .data("departments", queryShowcaseService.distinctDepartments())
                .data("count", queryShowcaseService.count())
                .data("totalCount", queryShowcaseService.count())
                .data("currentPage", null)
                .data("pageSize", null)
                .data("totalPages", null)
                .data("filterDepartment", null)
                .data("filterMinSalary", null)
                .data("filterMaxSalary", null)
                .data("filterNamePattern", null)
                .data("filterActive", null)
                .data("resultCount", null)
                .data("sortField", null)
                .data("sortDir", null)
                .data("streamResults", null)
                .data("streamDepartment", null)
                .data("streamMinSalary", null)
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    @POST
    @Path("/filter")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response filter(
            @FormParam("department") String department,
            @FormParam("minSalary") Double minSalary,
            @FormParam("maxSalary") Double maxSalary,
            @FormParam("namePattern") String namePattern,
            @FormParam("active") Boolean active,
            @Context HttpHeaders headers) {

        List<Employee> results;
        if (department != null && !department.isBlank()) {
            results = queryShowcaseService.findByDepartment(department);
        } else if (minSalary != null && maxSalary != null) {
            results = queryShowcaseService.findBySalaryRange(minSalary, maxSalary);
        } else if (namePattern != null && !namePattern.isBlank()) {
            results = queryShowcaseService.findByNamePattern(namePattern);
        } else if (active != null && active) {
            results = queryShowcaseService.findActive();
        } else {
            results = queryShowcaseService.findAll();
        }

        TemplateInstance ti = demoQuery.data("employees", results)
                .data("departments", queryShowcaseService.distinctDepartments())
                .data("count", queryShowcaseService.count())
                .data("totalCount", queryShowcaseService.count())
                .data("filterDepartment", department)
                .data("filterMinSalary", minSalary)
                .data("filterMaxSalary", maxSalary)
                .data("filterNamePattern", namePattern)
                .data("filterActive", active)
                .data("resultCount", results.size())
                .data("currentPage", null).data("pageSize", null).data("totalPages", null)
                .data("sortField", null).data("sortDir", null)
                .data("streamResults", null).data("streamDepartment", null).data("streamMinSalary", null)
                .data("successMessage", null).data("errorMessage", null)
                .data("lastOperation", "query.f(...).gte(...).sort(...).limit(...).asList()")
                .data("lastMongoCommand", "db.employees.find({...}).sort({...}).limit(N)");

        if (isHtmx(headers)) return Response.ok(ti).build();
        return Response.ok(query.data("active", "query").data("docLinks", DOC_LINKS)).build();
    }

    @POST
    @Path("/paginate")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response paginate(
            @FormParam("page") @DefaultValue("0") int page,
            @FormParam("size") @DefaultValue("5") int size,
            @FormParam("sortField") @DefaultValue("lastName") String sortField,
            @FormParam("sortDir") @DefaultValue("1") int sortDir,
            @Context HttpHeaders headers) {

        List<Employee> results = queryShowcaseService.findPaginated(page, size, sortField, sortDir);
        long totalCount = queryShowcaseService.count();
        int totalPages = (int) Math.ceil((double) totalCount / size);

        TemplateInstance ti = demoQuery.data("employees", results)
                .data("departments", queryShowcaseService.distinctDepartments())
                .data("count", totalCount).data("totalCount", totalCount)
                .data("currentPage", page).data("pageSize", size)
                .data("sortField", sortField).data("sortDir", sortDir)
                .data("totalPages", totalPages).data("resultCount", results.size())
                .data("filterDepartment", null).data("filterMinSalary", null)
                .data("filterMaxSalary", null).data("filterNamePattern", null)
                .data("filterActive", null)
                .data("streamResults", null).data("streamDepartment", null).data("streamMinSalary", null)
                .data("successMessage", null).data("errorMessage", null)
                .data("lastOperation", "query.f(...).gte(...).sort(...).limit(...).asList()")
                .data("lastMongoCommand", "db.employees.find({...}).sort({...}).limit(N)");

        if (isHtmx(headers)) return Response.ok(ti).build();
        return Response.ok(query.data("active", "query").data("docLinks", DOC_LINKS)).build();
    }

    @POST
    @Path("/stream")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response streamDemo(
            @FormParam("streamDepartment") String department,
            @FormParam("streamMinSalary") @DefaultValue("0") double minSalary,
            @Context HttpHeaders headers) {

        if (department == null || department.isBlank()) department = "Engineering";
        List<String> names = queryShowcaseService.streamHighEarnerNames(department, minSalary);

        TemplateInstance ti = demoQuery.data("employees", queryShowcaseService.findAll())
                .data("departments", queryShowcaseService.distinctDepartments())
                .data("count", queryShowcaseService.count())
                .data("totalCount", queryShowcaseService.count())
                .data("streamResults", names)
                .data("streamDepartment", department)
                .data("streamMinSalary", minSalary)
                .data("currentPage", null).data("pageSize", null).data("totalPages", null)
                .data("filterDepartment", null).data("filterMinSalary", null)
                .data("filterMaxSalary", null).data("filterNamePattern", null)
                .data("filterActive", null).data("resultCount", null)
                .data("sortField", null).data("sortDir", null)
                .data("successMessage", null).data("errorMessage", null)
                .data("lastOperation", "query.f(...).gte(...).sort(...).limit(...).asList()")
                .data("lastMongoCommand", "db.employees.find({...}).sort({...}).limit(N)");

        if (isHtmx(headers)) return Response.ok(ti).build();
        return Response.ok(query.data("active", "query").data("docLinks", DOC_LINKS)).build();
    }

    @POST
    @Path("/seed")
    public Response seed(@Context HttpHeaders headers) {
        queryShowcaseService.seedData();
        if (isHtmx(headers)) return Response.ok(demoAll("Sample data re-seeded.", null,
                "morphium.storeList(employees)", "db.employees.insertMany([...])")).build();
        return Response.seeOther(URI.create("/query")).build();
    }
}
