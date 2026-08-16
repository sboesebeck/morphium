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
package de.caluga.morphium.showcase.migration;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.quarkus.migration.Execution;
import de.caluga.morphium.quarkus.migration.MorphiumChangeUnit;
import de.caluga.morphium.quarkus.migration.RollbackExecution;
import de.caluga.morphium.showcase.migration.entity.MigrationNote;

/**
 * Demo {@code @MorphiumChangeUnit}: backfills {@code archived = false} on every
 * {@link MigrationNote} document that does not have the field set yet.
 *
 * <p>This is deliberately idempotent, which the migration runner requires: running it twice
 * is harmless, because the {@code $set} update only ever targets documents where
 * {@code archived} is still absent. Once every document has been backfilled, the query
 * matches nothing and the migration is effectively a no-op on subsequent runs &mdash; the
 * runner's changelog additionally ensures it never re-executes {@code execute()} for the
 * same {@code id} once it is recorded as {@code EXECUTED} anyway.</p>
 *
 * <p>{@code order = "001"} is zero-padded, following the convention recommended by the
 * migration runner's own javadoc: {@code order} is compared numerically when both sides parse
 * as a number, but falls back to lexicographic comparison otherwise, so zero-padding avoids
 * {@code "10"} sorting before {@code "2"} once there are more than nine migrations.</p>
 */
@MorphiumChangeUnit(id = "001-add-archived-flag", order = "001", author = "showcase")
public class AddArchivedFlagMigration {

    @Execution
    public void execute(Morphium morphium) {
        Query<MigrationNote> query = morphium.createQueryFor(MigrationNote.class)
                .f(MigrationNote.Fields.archived).eq(null);
        query.set(MigrationNote.Fields.archived, false, false, true, null);
    }

    /**
     * Rollback is symmetric: clear the field again on every document this migration touched.
     * There is no way to distinguish "we set it" from "it was already false" after the fact,
     * so rollback conservatively unsets the field on every document currently {@code false}
     * &mdash; acceptable here because this is a demo, but a production migration touching
     * ambiguous state like this would typically record which document IDs it modified.
     */
    @RollbackExecution
    public void rollback(Morphium morphium) {
        Query<MigrationNote> query = morphium.createQueryFor(MigrationNote.class)
                .f(MigrationNote.Fields.archived).eq(false);
        query.set(MigrationNote.Fields.archived, null, false, true, null);
    }
}
