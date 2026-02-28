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

import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Root JAX-RS resource serving the showcase application's landing page.
 *
 * <p>This is the entry point of the Quarkus Morphium Showcase web application. It handles
 * requests to the root URL ({@code /}) and renders the home page using the Qute templating
 * engine. The home page provides an overview of the available showcase modules (polymorphism,
 * catalog, bank, etc.) and links to each one.</p>
 *
 * <p>Quarkus automatically discovers this class as a JAX-RS resource through classpath scanning --
 * no explicit registration in an {@code Application} subclass is needed. The {@code @Path("/")}
 * annotation maps it to the web root.</p>
 *
 * <p>This resource does not interact with Morphium or MongoDB directly. It serves purely as
 * the navigation hub for the showcase application.</p>
 */
@Path("/")
public class ShowcaseApplication {

    /**
     * The Qute template for the home/index page.
     * By convention, Quarkus Qute resolves this to the template file {@code templates/index.html}
     * based on the field name "index".
     */
    @Inject
    Template index;

    /**
     * Renders the showcase landing page.
     *
     * <p>Passes the "active" navigation identifier to the template so the shared layout
     * can highlight the correct navigation item. The value "home" indicates the home page
     * is the currently active section.</p>
     *
     * @return a Qute template instance that will be rendered as HTML
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance home() {
        return index.data("active", "home");
    }
}