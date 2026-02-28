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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class MarkdownService {

    private static final Logger LOG = Logger.getLogger(MarkdownService.class);
    private static final String DOCS_RESOURCE_PATH = "docs";
    private static final Pattern LINK_PATTERN = Pattern.compile("\\[([^]]+)]\\(\\./([^)]+)\\.md\\)");
    private static final Pattern HEADING_PATTERN = Pattern.compile("^#\\s+(.+)$", Pattern.MULTILINE);

    private final Parser parser;
    private final HtmlRenderer renderer;
    private final Path docsRoot;

    public MarkdownService() {
        MutableDataSet options = new MutableDataSet();
        options.set(Parser.EXTENSIONS, List.of(
                TablesExtension.create(),
                AutolinkExtension.create(),
                StrikethroughExtension.create()
        ));
        this.parser = Parser.builder(options).build();
        this.renderer = HtmlRenderer.builder(options).build();
        this.docsRoot = resolveDocsRoot();
        LOG.infof("Documentation root resolved to: %s (exists: %s)", docsRoot, Files.isDirectory(docsRoot));
    }

    private static Path resolveDocsRoot() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(DOCS_RESOURCE_PATH);
        if (resource != null) {
            try {
                return Path.of(resource.toURI());
            } catch (URISyntaxException e) {
                LOG.warnf("Could not resolve docs classpath resource: %s", e.getMessage());
            }
        }
        // Fallback: try relative to working directory
        return Path.of(DOCS_RESOURCE_PATH);
    }

    public Optional<String> renderDoc(String slug) {
        Path file = docsRoot.resolve(slug + ".md");
        if (!Files.isRegularFile(file)) {
            return Optional.empty();
        }
        try {
            String markdown = Files.readString(file);
            markdown = rewriteLinks(markdown);
            Node document = parser.parse(markdown);
            return Optional.of(renderer.render(document));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<String> extractTitle(String slug) {
        Path file = docsRoot.resolve(slug + ".md");
        if (!Files.isRegularFile(file)) {
            return Optional.empty();
        }
        try {
            String markdown = Files.readString(file);
            Matcher m = HEADING_PATTERN.matcher(markdown);
            if (m.find()) {
                return Optional.of(m.group(1).trim());
            }
            return Optional.of(slug);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public List<DocEntry> listDocs() {
        List<DocEntry> entries = new ArrayList<>();
        if (!Files.isDirectory(docsRoot)) {
            return entries;
        }
        collectDocs(docsRoot, "", entries);
        entries.sort(Comparator.comparing(DocEntry::category).thenComparing(DocEntry::title));
        return entries;
    }

    private void collectDocs(Path dir, String prefix, List<DocEntry> entries) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    String subPrefix = prefix.isEmpty()
                            ? entry.getFileName().toString()
                            : prefix + "/" + entry.getFileName().toString();
                    collectDocs(entry, subPrefix, entries);
                } else if (entry.toString().endsWith(".md")) {
                    String fileName = entry.getFileName().toString();
                    String slug = prefix.isEmpty()
                            ? fileName.replace(".md", "")
                            : prefix + "/" + fileName.replace(".md", "");
                    String category = prefix.isEmpty() ? "General" : capitalize(prefix.replace("/", " > "));
                    String title = extractTitleFromFile(entry).orElse(slug);
                    entries.add(new DocEntry(slug, title, category));
                }
            }
        } catch (IOException e) {
            // skip unreadable directories
        }
    }

    private Optional<String> extractTitleFromFile(Path file) {
        try {
            String content = Files.readString(file);
            Matcher m = HEADING_PATTERN.matcher(content);
            if (m.find()) {
                return Optional.of(m.group(1).trim());
            }
        } catch (IOException e) {
            // ignore
        }
        return Optional.empty();
    }

    private String rewriteLinks(String markdown) {
        return LINK_PATTERN.matcher(markdown).replaceAll("[$1](/docs/$2)");
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}