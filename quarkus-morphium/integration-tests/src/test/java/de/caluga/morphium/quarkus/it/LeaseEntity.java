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

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;

import java.util.Date;

/** A lease for the {@link LeaseRepository} (MOR-021): who holds it, until when, and the fencing token that grows with every acquisition. */
@Entity(collectionName = "it_leases")
public class LeaseEntity {
    @Id
    private String id;
    private String owner;
    private Date expiresAt;
    private long fencing;
    private String createdBy;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }
    public Date getExpiresAt() { return expiresAt; }
    public void setExpiresAt(Date expiresAt) { this.expiresAt = expiresAt; }
    public long getFencing() { return fencing; }
    public void setFencing(long fencing) { this.fencing = fencing; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
}
