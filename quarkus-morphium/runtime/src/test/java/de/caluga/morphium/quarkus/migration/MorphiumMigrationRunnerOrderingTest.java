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
package de.caluga.morphium.quarkus.migration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link MorphiumMigrationRunner#compareByOrder}.
 *
 * <p>Previously, migrations were sorted with {@code Comparator.comparing(MigrationInfo::order)},
 * a plain lexicographic string comparison. {@code order()} is a {@code String}, so "10" sorts
 * BEFORE "2" lexicographically -- this codebase's own tests never caught it because every
 * existing test migration happens to use a zero-padded, equal-width order string ("001", "002",
 * "999"). A real project with more than 9 migrations and unpadded order values would see them
 * silently run out of order.
 */
@DisplayName("MorphiumMigrationRunner — migration ordering (should-fix #9)")
class MorphiumMigrationRunnerOrderingTest {

    private static MorphiumMigrationRunner.MigrationInfo info(String order) {
        return new MorphiumMigrationRunner.MigrationInfo(
                "id-" + order, order, "test", "TestClass", Object.class, null, null);
    }

    @Test
    @DisplayName("numeric order values sort numerically, not lexicographically: \"2\" before \"10\"")
    void numericOrderValues_sortNumerically() {
        List<MorphiumMigrationRunner.MigrationInfo> migrations = new ArrayList<>(List.of(
                info("10"), info("2"), info("1")));

        migrations.sort(MorphiumMigrationRunner::compareByOrder);

        assertThat(migrations).extracting(MorphiumMigrationRunner.MigrationInfo::order)
                .containsExactly("1", "2", "10");
    }

    @Test
    @DisplayName("zero-padded order values (the existing test-suite convention) still sort correctly")
    void zeroPaddedOrderValues_stillSortCorrectly() {
        List<MorphiumMigrationRunner.MigrationInfo> migrations = new ArrayList<>(List.of(
                info("999"), info("001"), info("002")));

        migrations.sort(MorphiumMigrationRunner::compareByOrder);

        assertThat(migrations).extracting(MorphiumMigrationRunner.MigrationInfo::order)
                .containsExactly("001", "002", "999");
    }

    @Test
    @DisplayName("non-numeric order values fall back to lexicographic comparison")
    void nonNumericOrderValues_fallBackToLexicographic() {
        List<MorphiumMigrationRunner.MigrationInfo> migrations = new ArrayList<>(List.of(
                info("2024-06-01"), info("2024-01-01"), info("2024-03-01")));

        migrations.sort(MorphiumMigrationRunner::compareByOrder);

        assertThat(migrations).extracting(MorphiumMigrationRunner.MigrationInfo::order)
                .containsExactly("2024-01-01", "2024-03-01", "2024-06-01");
    }

    @Test
    @DisplayName("a mix of numeric and non-numeric order values does not throw")
    void mixedNumericAndNonNumeric_doesNotThrow() {
        List<MorphiumMigrationRunner.MigrationInfo> migrations = new ArrayList<>(List.of(
                info("10"), info("abc")));

        // Must not throw NumberFormatException -- just document it doesn't crash;
        // mixed conventions within one project are a user error, not something to optimize for.
        migrations.sort(MorphiumMigrationRunner::compareByOrder);
        assertThat(migrations).hasSize(2);
    }
}
