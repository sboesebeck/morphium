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
 * Immutable data transfer object carrying metadata about the currently rendered page.
 *
 * <p>Used by the Qute template layout to set the HTML page title and to determine which
 * navigation item should be highlighted as "active". Each JAX-RS resource passes a
 * {@code PageInfo} (or its individual fields) into the template context so the shared
 * layout can render the correct navigation state.</p>
 *
 * <p>Like {@link DocLink}, this is a presentation-layer DTO with no connection to Morphium
 * or MongoDB.</p>
 *
 * @param title  the page title displayed in the browser tab and page header
 * @param active the identifier of the currently active navigation item (e.g. "polymorphism", "docs")
 */
public record PageInfo(String title, String active) {

    /**
     * Factory method for convenient construction.
     *
     * @param title  the page title
     * @param active the active navigation identifier
     * @return a new {@code PageInfo} instance
     */
    public static PageInfo of(String title, String active) {
        return new PageInfo(title, active);
    }
}