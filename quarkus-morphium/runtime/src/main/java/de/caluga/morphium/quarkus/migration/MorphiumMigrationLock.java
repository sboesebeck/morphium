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
 * Distributed lock entity for migration execution. Only one instance
 * of the lock document (with a fixed {@code _id}) exists at a time.
 * The lock contains an expiration timestamp used to treat the lock as
 * expired and allow overriding stale locks after the configured TTL,
 * helping to prevent deadlocks from crashed processes.
 */
@Entity(collectionName = "morphiumMigrationLock")
public class MorphiumMigrationLock {

    @Id
    private String id;

    @Property(fieldName = "owner")
    private String owner;

    @Property(fieldName = "acquired_at")
    private Date acquiredAt;

    @Property(fieldName = "expires_at")
    private Date expiresAt;

    public MorphiumMigrationLock() {
    }

    // --- accessors ---

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public Date getAcquiredAt() { return acquiredAt; }
    public void setAcquiredAt(Date acquiredAt) { this.acquiredAt = acquiredAt; }

    public Date getExpiresAt() { return expiresAt; }
    public void setExpiresAt(Date expiresAt) { this.expiresAt = expiresAt; }
}
