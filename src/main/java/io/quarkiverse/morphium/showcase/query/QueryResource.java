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

/**
 * JAX-RS resource exposing the Morphium Query API showcase as an HTML UI.
 *
 * <p>This resource provides a web interface for exploring Morphium's query capabilities
 * interactively. Users can apply filters (department, salary range, name pattern, active status)
 * and experiment with server-side pagination and sorting -- all backed by Morphium queries
 * against MongoDB.</p>
 *
 * <p><b>Morphium query features accessible through this UI:</b></p>
 * <ul>
 *   <li>Equality filters ({@code eq}) -- filter by department</li>
 *   <li>Range queries ({@code gte}, {@code lte}) -- filter by salary range</li>
 *   <li>Regex matching ({@code matches}) -- search by name pattern</li>
 *   <li>Boolean filters ({@code eq(true)}) -- filter by active status</li>
 *   <li>Server-side pagination ({@code skip/limit}) -- paginate through results</li>
 *   <li>Server-side sorting ({@code sort}) -- sort by any field in either direction</li>
 *   <li>Distinct values ({@code distinct}) -- populate department dropdown</li>
 *   <li>Server-side counting ({@code countAll}) -- display total counts</li>
 * </ul>
 */
@Path("/query")
public class QueryResource {

    @Inject
    Template query;

    @Inject
    QueryShowcaseService queryShowcaseService;

    /** Links to Morphium documentation relevant to the query features shown on this page. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/api-reference", "API Reference", "Query API: eq, gte, lte, matches, in, nin"),
            new DocLink("/docs/developer-guide", "Developer Guide", "@Aliases, @Transient, @FieldNameConstants"),
            new DocLink("/docs/howtos/field-names", "Field Names", "Type-safe Field References")
    );

    /**
     * Renders the query showcase page with all employees and no filters applied.
     *
     * <p>Seeds sample data on first access. The template receives all employees, a list
     * of distinct departments (for the filter dropdown), and various null placeholders
     * for filter/pagination state that the template uses to show/hide UI elements.</p>
     *
     * @return a Qute template instance populated with employees and metadata
     */
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

    /**
     * Applies a filter and returns matching employees.
     *
     * <p>Demonstrates how different Morphium query operators are selected based on user input.
     * The filter priority is: department (equality) > salary range (gte/lte) > name pattern
     * (regex) > active status (boolean equality). Only one filter is applied at a time to
     * keep the demo clear and easy to understand.</p>
     *
     * @param department  optional department name for equality filter
     * @param minSalary   optional minimum salary for range filter
     * @param maxSalary   optional maximum salary for range filter
     * @param namePattern optional regex pattern for name matching
     * @param active      optional boolean to filter active employees only
     * @return a Qute template instance populated with filtered results
     */
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

    /**
     * Returns a paginated and sorted page of employees.
     *
     * <p>Demonstrates Morphium's server-side pagination with {@code skip()} and {@code limit()}.
     * The page number is zero-based; the skip offset is calculated as {@code page * size}.
     * Sort field and direction are configurable to showcase Morphium's flexible sorting.</p>
     *
     * @param page      zero-based page number (default: 0)
     * @param size      number of records per page (default: 5)
     * @param sortField field name to sort by (default: "lastName")
     * @param sortDir   sort direction: 1 = ascending, -1 = descending (default: 1)
     * @return a Qute template instance populated with the requested page of results
     */
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

    /**
     * Re-seeds the employee collection with sample data.
     *
     * @return a redirect response to the query showcase page
     */
    @POST
    @Path("/seed")
    public Response seed() {
        queryShowcaseService.seedData();
        return Response.seeOther(URI.create("/query")).build();
    }
}
