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

/**
 * CDI service for discovering, parsing, and rendering Markdown documentation files.
 *
 * <p>This service powers the showcase's built-in documentation hub. It scans the classpath
 * {@code docs/} directory for {@code .md} files, parses them using the
 * <a href="https://github.com/vsch/flexmark-java">flexmark-java</a> Markdown library, and
 * renders them as HTML. It also handles link rewriting so that relative Markdown links
 * (e.g. {@code ./other-doc.md}) are transformed into the correct web URLs.</p>
 *
 * <p>This service has no connection to Morphium or MongoDB -- it is a utility for the
 * showcase's educational content layer.</p>
 */
@ApplicationScoped
public class MarkdownService {

    private static final Logger LOG = Logger.getLogger(MarkdownService.class);

    /** Classpath resource path where Markdown documentation files are located. */
    private static final String DOCS_RESOURCE_PATH = "docs";

    /**
     * Regex matching relative Markdown links like {@code [Title](./other-doc.md)}.
     * Used by {@link #rewriteLinks(String)} to convert them to web-friendly URLs.
     */
    private static final Pattern LINK_PATTERN = Pattern.compile("\\[([^]]+)]\\(\\./([^)]+)\\.md\\)");

    /** Regex matching the first level-1 Markdown heading ({@code # Title}) in a file. */
    private static final Pattern HEADING_PATTERN = Pattern.compile("^#\\s+(.+)$", Pattern.MULTILINE);

    /** Flexmark Markdown parser configured with GFM-compatible extensions. */
    private final Parser parser;

    /** Flexmark HTML renderer that converts parsed Markdown AST nodes into HTML. */
    private final HtmlRenderer renderer;

    /** Resolved filesystem path to the docs root directory. */
    private final Path docsRoot;

    /**
     * Initializes the Markdown parser and renderer with GitHub Flavored Markdown extensions
     * (tables, autolinks, strikethrough) and resolves the documentation root directory.
     */
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

    /**
     * Resolves the filesystem path to the documentation root directory.
     * First tries the classpath resource, then falls back to a relative path from the working directory.
     *
     * @return the resolved path to the docs directory
     */
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

    /**
     * Renders a Markdown documentation file identified by its slug into HTML.
     *
     * <p>The slug is resolved to a {@code .md} file under the docs root directory.
     * Before parsing, relative Markdown links are rewritten to web-friendly URLs.</p>
     *
     * @param slug the URL-friendly path segment (e.g. "developer-guide" or "advanced/custom-mappers")
     * @return the rendered HTML content, or empty if the file does not exist or cannot be read
     */
    public Optional<String> renderDoc(String slug) {
        Path file = docsRoot.resolve(slug + ".md");
        if (!Files.isRegularFile(file)) {
            return Optional.empty();
        }
        try {
            String markdown = Files.readString(file);
            // Rewrite relative Markdown links (./other.md) to absolute web URLs (/docs/other)
            markdown = rewriteLinks(markdown);
            Node document = parser.parse(markdown);
            return Optional.of(renderer.render(document));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    /**
     * Extracts the title (first level-1 heading) from a Markdown documentation file.
     *
     * @param slug the URL-friendly path segment identifying the document
     * @return the extracted title, or the slug itself as fallback, or empty if the file does not exist
     */
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

    /**
     * Discovers all Markdown documentation files and returns them as a sorted list of {@link DocEntry} records.
     *
     * <p>The docs root directory is scanned recursively. Each {@code .md} file becomes a
     * {@code DocEntry} with a slug derived from its path, a title extracted from its first heading,
     * and a category derived from its parent subdirectory. Results are sorted by category, then title.</p>
     *
     * @return a sorted list of all discovered documentation entries
     */
    public List<DocEntry> listDocs() {
        List<DocEntry> entries = new ArrayList<>();
        if (!Files.isDirectory(docsRoot)) {
            return entries;
        }
        collectDocs(docsRoot, "", entries);
        entries.sort(Comparator.comparing(DocEntry::category).thenComparing(DocEntry::title));
        return entries;
    }

    /**
     * Recursively collects Markdown files from the given directory into the entries list.
     *
     * @param dir     the directory to scan
     * @param prefix  the accumulated path prefix for slug construction (empty for the root)
     * @param entries the accumulator list to add discovered entries to
     */
    private void collectDocs(Path dir, String prefix, List<DocEntry> entries) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    // Build the path prefix for subdirectory-based categorization
                    String subPrefix = prefix.isEmpty()
                            ? entry.getFileName().toString()
                            : prefix + "/" + entry.getFileName().toString();
                    collectDocs(entry, subPrefix, entries);
                } else if (entry.toString().endsWith(".md")) {
                    String fileName = entry.getFileName().toString();
                    // Construct the URL slug from the prefix and filename (without .md extension)
                    String slug = prefix.isEmpty()
                            ? fileName.replace(".md", "")
                            : prefix + "/" + fileName.replace(".md", "");
                    // Derive the category from the subdirectory path, or "General" for root-level files
                    String category = prefix.isEmpty() ? "General" : capitalize(prefix.replace("/", " > "));
                    String title = extractTitleFromFile(entry).orElse(slug);
                    entries.add(new DocEntry(slug, title, category));
                }
            }
        } catch (IOException e) {
            // skip unreadable directories
        }
    }

    /**
     * Extracts the first level-1 heading from a Markdown file to use as the document title.
     *
     * @param file the path to the Markdown file
     * @return the extracted title, or empty if no heading is found or the file cannot be read
     */
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

    /**
     * Rewrites relative Markdown links to absolute web URLs.
     *
     * <p>Transforms patterns like {@code [Link Text](./other-doc.md)} into
     * {@code [Link Text](/docs/other-doc)} so they work as web links in the rendered HTML.</p>
     *
     * @param markdown the raw Markdown content
     * @return the Markdown content with rewritten links
     */
    private String rewriteLinks(String markdown) {
        return LINK_PATTERN.matcher(markdown).replaceAll("[$1](/docs/$2)");
    }

    /**
     * Capitalizes the first character of a string.
     *
     * @param s the input string
     * @return the string with its first character in upper case, or the original if null/empty
     */
    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}