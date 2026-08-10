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
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PreStore;

/**
 * Minimal test entity used across all integration tests.
 * Exercises @Entity, @Id, @Property, @Version and @Lifecycle / @PreStore.
 */
@Entity(collectionName = "it_items")
@Lifecycle
public class ItemEntity {

    @Id
    private String id;

    @Property(fieldName = "name")
    private String name;

    @Property(fieldName = "price")
    private double price;

    @Version
    @Property(fieldName = "version")
    private long version;

    @Property(fieldName = "tag")
    private String tag;

    @PreStore
    public void onStore() {
        if (tag == null) tag = "default";
    }

    // --- accessors ---

    public String getId()             { return id; }
    public void   setId(String id)    { this.id = id; }

    public String getName()              { return name; }
    public void   setName(String name)   { this.name = name; }

    public double getPrice()               { return price; }
    public void   setPrice(double price)   { this.price = price; }

    public long   getVersion()               { return version; }
    public void   setVersion(long version)   { this.version = version; }

    public String getTag()             { return tag; }
    public void   setTag(String tag)   { this.tag = tag; }
}
