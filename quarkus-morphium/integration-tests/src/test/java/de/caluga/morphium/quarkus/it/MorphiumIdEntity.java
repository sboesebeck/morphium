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
import de.caluga.morphium.driver.MorphiumId;

/**
 * Test entity whose primary key is a {@link MorphiumId} — the shape that, without
 * a JSON customizer, leaks the internal {@code {pid, counter, ...}} struct over REST.
 */
@Entity(collectionName = "it_morphium_id")
public class MorphiumIdEntity {

    @Id
    private MorphiumId id;

    @Property(fieldName = "name")
    private String name;

    public MorphiumId getId()             { return id; }
    public void      setId(MorphiumId id) { this.id = id; }

    public String getName()            { return name; }
    public void   setName(String name) { this.name = name; }
}
