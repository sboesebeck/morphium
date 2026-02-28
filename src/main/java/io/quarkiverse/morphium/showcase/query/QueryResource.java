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
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

@Path("/query")
public class QueryResource {

    @Inject
    Template query;

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
                .data("employees", queryShowcaseService.findAll())
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
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/filter")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance filter(
            @FormParam("department") String department,
            @FormParam("minSalary") Double minSalary,
            @FormParam("maxSalary") Double maxSalary,
            @FormParam("namePattern") String namePattern,
            @FormParam("active") Boolean active) {

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

        return query.data("active", "query")
                .data("employees", results)
                .data("departments", queryShowcaseService.distinctDepartments())
                .data("count", queryShowcaseService.count())
                .data("totalCount", queryShowcaseService.count())
                .data("filterDepartment", department)
                .data("filterMinSalary", minSalary)
                .data("filterMaxSalary", maxSalary)
                .data("filterNamePattern", namePattern)
                .data("filterActive", active)
                .data("resultCount", results.size())
                .data("currentPage", null)
                .data("pageSize", null)
                .data("totalPages", null)
                .data("sortField", null)
                .data("sortDir", null)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/paginate")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance paginate(
            @FormParam("page") @DefaultValue("0") int page,
            @FormParam("size") @DefaultValue("5") int size,
            @FormParam("sortField") @DefaultValue("lastName") String sortField,
            @FormParam("sortDir") @DefaultValue("1") int sortDir) {

        List<Employee> results = queryShowcaseService.findPaginated(page, size, sortField, sortDir);
        long totalCount = queryShowcaseService.count();
        int totalPages = (int) Math.ceil((double) totalCount / size);

        return query.data("active", "query")
                .data("employees", results)
                .data("departments", queryShowcaseService.distinctDepartments())
                .data("count", totalCount)
                .data("totalCount", totalCount)
                .data("currentPage", page)
                .data("pageSize", size)
                .data("sortField", sortField)
                .data("sortDir", sortDir)
                .data("totalPages", totalPages)
                .data("resultCount", results.size())
                .data("filterDepartment", null)
                .data("filterMinSalary", null)
                .data("filterMaxSalary", null)
                .data("filterNamePattern", null)
                .data("filterActive", null)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/seed")
    public Response seed() {
        queryShowcaseService.seedData();
        return Response.seeOther(URI.create("/query")).build();
    }
}