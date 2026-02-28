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
package io.quarkiverse.morphium.showcase.common;

/**
 * Immutable data transfer object representing a hyperlink to a documentation page.
 *
 * <p>Each showcase page (e.g. the polymorphism demo, the catalog demo) includes a list of
 * relevant documentation links. This record carries the URL, a human-readable title, and a
 * short description of the topics covered. It is rendered by the Qute templates in the sidebar
 * or footer of each showcase page.</p>
 *
 * <p>This is a plain Java record -- it is <em>not</em> a Morphium entity and is never stored
 * in MongoDB. It exists purely for the UI/presentation layer.</p>
 *
 * @param url         the relative URL to the documentation page (e.g. "/docs/developer-guide")
 * @param title       the display title for the link (e.g. "Developer Guide")
 * @param description a comma-separated summary of key topics covered by the linked page
 */
public record DocLink(String url, String title, String description) {
}