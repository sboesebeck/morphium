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

import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Node;
import com.vladsch.flexmark.util.data.MutableDataSet;
import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.ext.autolink.AutolinkExtension;
import com.vladsch.flexmark.ext.gfm.strikethrough.StrikethroughExtension;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CDI service for discovering, parsing, and rendering Markdown documentation files.
 *
 * <p>Uses classpath resource loading ({@code getResourceAsStream}) so it works both
 * in Quarkus dev mode (exploded classes) and production mode (fast-jar / uber-jar).</p>
 */
@ApplicationScoped
public class MarkdownService {

    private static final Logger LOG = Logger.getLogger(MarkdownService.class);

    private static final String DOCS_PREFIX = "docs/";
    private static final String DOCS_INDEX = "docs-index.txt";

    private static final Pattern LINK_PATTERN = Pattern.compile("\\[([^]]+)]\\(\\./([^)]+)\\.md\\)");
    private static final Pattern HEADING_PATTERN = Pattern.compile("^#\\s+(.+)$", Pattern.MULTILINE);

    private final Parser parser;
    private final HtmlRenderer renderer;
    private final List<String> docPaths;

    public MarkdownService() {
        MutableDataSet options = new MutableDataSet();
        options.set(Parser.EXTENSIONS, List.of(
                TablesExtension.create(),
                AutolinkExtension.create(),
                StrikethroughExtension.create()
        ));
        this.parser = Parser.builder(options).build();
        this.renderer = HtmlRenderer.builder(options).build();
        this.docPaths = loadIndex();
        LOG.infof("Documentation index loaded: %d entries", docPaths.size());
    }

    private List<String> loadIndex() {
        List<String> paths = new ArrayList<>();
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(DOCS_INDEX)) {
            if (is == null) {
                LOG.warn("docs-index.txt not found on classpath");
                return paths;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        paths.add(line);
                    }
                }
            }
        } catch (IOException e) {
            LOG.warnf("Failed to read docs-index.txt: %s", e.getMessage());
        }
        return paths;
    }

    private Optional<String> readResource(String classpathPath) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathPath)) {
            if (is == null) {
                return Optional.empty();
            }
            return Optional.of(new String(is.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<String> renderDoc(String slug) {
        String resourcePath = DOCS_PREFIX + slug + ".md";
        return readResource(resourcePath).map(markdown -> {
            markdown = rewriteLinks(markdown);
            Node document = parser.parse(markdown);
            return renderer.render(document);
        });
    }

    public Optional<String> extractTitle(String slug) {
        String resourcePath = DOCS_PREFIX + slug + ".md";
        return readResource(resourcePath).map(markdown -> {
            Matcher m = HEADING_PATTERN.matcher(markdown);
            return m.find() ? m.group(1).trim() : slug;
        });
    }

    public List<DocEntry> listDocs() {
        List<DocEntry> entries = new ArrayList<>();
        for (String path : docPaths) {
            // path format: "docs/slug.md" or "docs/subdir/slug.md"
            String withoutPrefix = path.startsWith(DOCS_PREFIX) ? path.substring(DOCS_PREFIX.length()) : path;
            String slug = withoutPrefix.replace(".md", "");

            int lastSlash = slug.lastIndexOf('/');
            String category = lastSlash < 0 ? "General" : capitalize(slug.substring(0, lastSlash).replace("/", " > "));
            String title = readResource(path)
                    .flatMap(content -> {
                        Matcher m = HEADING_PATTERN.matcher(content);
                        return m.find() ? Optional.of(m.group(1).trim()) : Optional.empty();
                    })
                    .orElse(slug);
            entries.add(new DocEntry(slug, title, category));
        }
        entries.sort(Comparator.comparing(DocEntry::category).thenComparing(DocEntry::title));
        return entries;
    }

    private String rewriteLinks(String markdown) {
        return LINK_PATTERN.matcher(markdown).replaceAll("[$1](/docs/$2)");
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}
