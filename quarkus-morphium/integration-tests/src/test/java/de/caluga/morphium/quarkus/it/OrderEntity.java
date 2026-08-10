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

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.lifecycle.*;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Test entity used in query and LocalDateTime integration tests.
 */
@Entity(collectionName = "it_orders")
@Lifecycle
public class OrderEntity {

    @Id
    private String id;

    @Property(fieldName = "customer_id")
    private String customerId;

    @Property(fieldName = "amount")
    private double amount;

    @Property(fieldName = "status")
    private String status;

    @Property(fieldName = "created_at")
    private LocalDateTime createdAt;

    @Property(fieldName = "tags")
    private List<String> tags;

    @Property(fieldName = "urgent")
    private boolean urgent;

    @Version
    @Property(fieldName = "version")
    private long version;

    @PreStore
    public void onStore() {
        if (createdAt == null) createdAt = LocalDateTime.now();
    }

    public String getId()                        { return id; }
    public void   setId(String id)               { this.id = id; }
    public String getCustomerId()                { return customerId; }
    public void   setCustomerId(String c)        { this.customerId = c; }
    public double getAmount()                    { return amount; }
    public void   setAmount(double a)            { this.amount = a; }
    public String getStatus()                    { return status; }
    public void   setStatus(String s)            { this.status = s; }
    public LocalDateTime getCreatedAt()          { return createdAt; }
    public void   setCreatedAt(LocalDateTime d)  { this.createdAt = d; }
    public List<String> getTags()                { return tags; }
    public void   setTags(List<String> t)        { this.tags = t; }
    public boolean isUrgent()                     { return urgent; }
    public void   setUrgent(boolean u)           { this.urgent = u; }
    public long   getVersion()                   { return version; }
    public void   setVersion(long v)             { this.version = v; }
}
