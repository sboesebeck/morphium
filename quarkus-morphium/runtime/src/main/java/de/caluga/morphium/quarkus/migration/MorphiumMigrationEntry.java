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

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;

import java.util.Date;

/**
 * Tracks applied database migrations. Each successfully executed
 * {@link MorphiumChangeUnit} produces one entry in this collection.
 */
@Entity(collectionName = "morphiumChangeLog")
public class MorphiumMigrationEntry {

    public enum ChangeState {
        EXECUTED,
        ROLLED_BACK,
        FAILED
    }

    @Id
    private String id;

    @Property(fieldName = "change_id")
    private String changeId;

    @Property(fieldName = "author")
    private String author;

    @Property(fieldName = "order")
    private String order;

    @Property(fieldName = "migration_class")
    private String className;

    @Property(fieldName = "executed_at")
    private Date executedAt;

    @Property(fieldName = "execution_time_ms")
    private long executionTimeMs;

    @Property(fieldName = "state")
    private ChangeState state;

    public MorphiumMigrationEntry() {
    }

    // --- accessors ---

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getChangeId() { return changeId; }
    public void setChangeId(String changeId) { this.changeId = changeId; }

    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }

    public String getOrder() { return order; }
    public void setOrder(String order) { this.order = order; }

    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }

    public Date getExecutedAt() { return executedAt; }
    public void setExecutedAt(Date executedAt) { this.executedAt = executedAt; }

    public long getExecutionTimeMs() { return executionTimeMs; }
    public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }

    public ChangeState getState() { return state; }
    public void setState(ChangeState state) { this.state = state; }
}
