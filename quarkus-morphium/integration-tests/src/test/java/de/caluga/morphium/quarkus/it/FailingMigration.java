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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.migration.Execution;
import de.caluga.morphium.quarkus.migration.MorphiumChangeUnit;
import de.caluga.morphium.quarkus.migration.RollbackExecution;

/**
 * Test migration that always fails. Used to verify rollback behavior.
 */
@MorphiumChangeUnit(id = "999-failing", order = "999", author = "test")
public class FailingMigration {

    public static volatile boolean rollbackExecuted = false;

    @Execution
    public void execute(Morphium morphium) {
        throw new RuntimeException("Intentional failure for rollback test");
    }

    @RollbackExecution
    public void rollback(Morphium morphium) {
        rollbackExecuted = true;
    }
}
