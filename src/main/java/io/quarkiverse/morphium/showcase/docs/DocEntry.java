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

/**
 * Immutable data transfer object representing a single documentation article in the docs hub.
 *
 * <p>Each Markdown file discovered in the classpath {@code docs/} directory is represented as a
 * {@code DocEntry}. The {@link MarkdownService} scans the directory tree, extracts the title from
 * each file's first {@code #} heading, and computes a URL-friendly slug and a category based on
 * the subdirectory structure.</p>
 *
 * <p>This record is used by the Qute templates to render the documentation index page, grouped
 * by category. It is a presentation-layer DTO with no connection to Morphium or MongoDB.</p>
 *
 * @param slug     the URL-friendly path segment used to link to the doc page (e.g. "developer-guide"
 *                 or "advanced/custom-mappers")
 * @param title    the human-readable title extracted from the Markdown file's first heading
 * @param category the category label derived from the subdirectory name (e.g. "General", "Advanced")
 */
public record DocEntry(String slug, String title, String category) {
}