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
package io.quarkiverse.morphium.showcase.analytics;

import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

/**
 * JAX-RS resource that exposes the analytics dashboard, demonstrating Morphium's
 * <strong>Aggregation Framework</strong> capabilities.
 *
 * <p>This resource serves an HTML page (via Quarkus Qute templates) that displays
 * various aggregation results: sales by region, sales by product, monthly trends,
 * and top sales representatives. All the actual Morphium interaction happens in
 * {@link AnalyticsService}; this resource simply orchestrates the data flow between
 * the service layer and the template engine.</p>
 *
 * <h3>Morphium Concepts Shown Indirectly</h3>
 * <ul>
 *   <li>Aggregation pipelines ({@code $group}, {@code $project}, {@code $sort}, {@code $limit})</li>
 *   <li>Distinct queries</li>
 *   <li>Document counting</li>
 *   <li>Bulk insert seeding</li>
 * </ul>
 *
 * @see AnalyticsService for the Morphium aggregation logic
 * @see io.quarkiverse.morphium.showcase.analytics.entity.SalesRecord the entity being aggregated
 */
@Path("/analytics")
public class AnalyticsResource {

    /** Qute template for the analytics HTML page. */
    @Inject
    Template analytics;

    /** Service containing all Morphium aggregation operations. */
    @Inject
    AnalyticsService analyticsService;

    /** Documentation links displayed on the analytics page to guide learners. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/howtos/aggregation-examples", "Aggregation Examples", "$group, $match, $sort, $project"),
            new DocLink("/docs/api-reference", "API Reference", "Aggregator, Expr, distinct()")
    );

    /**
     * Renders the analytics dashboard page.
     *
     * <p>On each request, this method first ensures sample data exists (via {@code seedData()}),
     * then executes multiple aggregation queries to populate the template with analytics data.
     * In a production application you would typically seed data separately, but for a showcase
     * this ensures the demo always has data to display.</p>
     *
     * @return a Qute template instance populated with aggregation results, ready to render as HTML
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        // Ensure demo data exists in MongoDB before running queries.
        analyticsService.seedData();

        // Each .data() call passes a named value to the Qute template for rendering.
        // The template iterates over these aggregation results to build charts and tables.
        return analytics.data("active", "analytics")
                .data("salesByRegion", analyticsService.salesByRegion())
                .data("salesByProduct", analyticsService.salesByProduct())
                .data("monthlySales", analyticsService.monthlySales())
                .data("topSalesReps", analyticsService.topSalesReps())
                .data("totalRecords", analyticsService.totalRecords())
                .data("distinctRegions", analyticsService.distinctRegions())
                .data("distinctProducts", analyticsService.distinctProducts())
                .data("docLinks", DOC_LINKS);
    }
}