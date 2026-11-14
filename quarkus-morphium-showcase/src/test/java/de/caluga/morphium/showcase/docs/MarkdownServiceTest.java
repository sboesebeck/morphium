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
package de.caluga.morphium.showcase.docs;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The link and admonition rewriting of the docs hub: plain functions, no application. */
class MarkdownServiceTest {

    @Test
    void relativeLinksAreResolvedAgainstThePage() {
        assertThat(MarkdownService.rewriteLinks("[a](../api-reference.md) [b](update-pitfalls.md#7) [c](./x.md)", "howtos/migration"))
                .isEqualTo("[a](/docs/api-reference) [b](/docs/howtos/update-pitfalls#7) [c](/docs/howtos/x)");
        // a link above the root of docs/ stays as it is
        assertThat(MarkdownService.rewriteLinks("[a](howtos/x.md) [b](../research/y.md)", "index"))
                .isEqualTo("[a](/docs/howtos/x) [b](../research/y.md)");
    }

    @Test
    void externalAnchorAndAbsoluteLinksStay() {
        String md = "[a](https://example.org/x.md) [b](#top) [c](/docs/x) [d](mailto:x@y.z) [e](image.png)";
        assertThat(MarkdownService.rewriteLinks(md, "howtos/x")).isEqualTo(md);
    }

    @Test
    void anAdmonitionBecomesABlockQuote() {
        String out = MarkdownService.rewriteAdmonitions("before\n\n!!! warning \"Careful\"\n    first line\n    second line\n\nafter\n");
        assertThat(out).contains("> **Careful**").contains("> first line").contains("> second line").contains("after").doesNotContain("!!!");
    }
}
