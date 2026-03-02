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

@Path("/analytics")
public class AnalyticsResource {

    @Inject
    Template analytics;

    @Inject
    @Location("tags/learn-analytics.html")
    Template learnAnalytics;

    @Inject
    @Location("tags/demo-analytics.html")
    Template demoAnalytics;

    @Inject
    AnalyticsService analyticsService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/howtos/aggregation-examples", "Aggregation Examples", "$group, $project, $sort, $bucket"),
            new DocLink("/docs/api-reference", "API Reference", "Aggregator, Expr, aggregate()"),
            new DocLink("/docs/developer-guide", "Developer Guide", "Aggregation Framework")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        analyticsService.seedData();
        return analytics.data("active", "analytics")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnAnalytics.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        analyticsService.seedData();
        return demoData(null, null);
    }

    private TemplateInstance demoData(String success, String error) {
        return demoAnalytics
                .data("salesByRegion", analyticsService.salesByRegion())
                .data("salesByProduct", analyticsService.salesByProduct())
                .data("monthlySales", analyticsService.monthlySales())
                .data("topSalesReps", analyticsService.topSalesReps())
                .data("distinctRegions", analyticsService.distinctRegions())
                .data("distinctProducts", analyticsService.distinctProducts())
                .data("totalRecords", analyticsService.totalRecords())
                .data("successMessage", success)
                .data("errorMessage", error);
    }

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed(@Context HttpHeaders headers) {
        analyticsService.resetData();
        if (headers.getHeaderString("HX-Request") != null) {
            return Response.ok(demoData("Sample data re-seeded.", null)).build();
        }
        return Response.seeOther(URI.create("/analytics")).build();
    }
}
