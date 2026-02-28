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
package io.quarkiverse.morphium.showcase.docs;

import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JAX-RS resource serving the built-in documentation hub for the Morphium showcase.
 *
 * <p>This resource provides two endpoints: a documentation index page that lists all available
 * Markdown articles grouped by category, and a detail page that renders a specific Markdown
 * article as HTML. The heavy lifting (Markdown parsing, file discovery) is delegated to
 * {@link MarkdownService}.</p>
 *
 * <p>This resource does not interact with Morphium or MongoDB -- it is a pure content-serving
 * layer for the showcase's educational documentation.</p>
 */
@Path("/docs")
public class DocsResource {

    /** Qute template for the documentation hub / index page. */
    @Inject
    Template docs;

    /**
     * Qute template for rendering a single documentation article.
     * The {@code @Location} annotation explicitly selects the "docs-page.html" template file,
     * overriding the default convention (which would look for a template named after the field).
     */
    @Inject
    @io.quarkus.qute.Location("docs-page.html")
    Template docsPage;

    /** Service responsible for discovering, parsing, and rendering Markdown documentation files. */
    @Inject
    MarkdownService markdownService;

    /**
     * Renders the documentation hub page listing all available articles grouped by category.
     *
     * <p>Articles are discovered from Markdown files in the classpath {@code docs/} directory.
     * They are grouped by their subdirectory (category) and sorted alphabetically within each group.</p>
     *
     * @return a Qute template instance with the grouped documentation entries
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance hub() {
        List<DocEntry> allDocs = markdownService.listDocs();

        // Group docs by category while preserving insertion order (LinkedHashMap).
        Map<String, List<DocEntry>> grouped = allDocs.stream()
                .collect(Collectors.groupingBy(DocEntry::category, LinkedHashMap::new, Collectors.toList()));
        return docs.data("active", "docs")
                .data("docsByCategory", grouped)
                .data("totalDocs", allDocs.size());
    }

    /**
     * Renders a single documentation article identified by its URL slug.
     *
     * <p>The slug can contain path separators (e.g. "advanced/custom-mappers") thanks to the
     * regex path parameter {@code {slug:.+}}. The Markdown content is converted to HTML and
     * injected into the docs-page template.</p>
     *
     * @param slug the URL-friendly identifier of the documentation article
     * @return a Qute template instance with the rendered HTML content and article title
     */
    @GET
    @Path("/{slug:.+}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page(@PathParam("slug") String slug) {
        String html = markdownService.renderDoc(slug).orElse("<p>Document not found: " + slug + "</p>");
        String title = markdownService.extractTitle(slug).orElse(slug);
        return docsPage.data("active", "docs")
                .data("docHtml", html)
                .data("docTitle", title)
                .data("slug", slug);
    }
}