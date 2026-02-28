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

@Path("/analytics")
public class AnalyticsResource {

    @Inject
    Template analytics;

    @Inject
    AnalyticsService analyticsService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/howtos/aggregation-examples", "Aggregation Examples", "$group, $match, $sort, $project"),
            new DocLink("/docs/api-reference", "API Reference", "Aggregator, Expr, distinct()")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        analyticsService.seedData();
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