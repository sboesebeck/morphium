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
import de.caluga.morphium.annotations.EnumType;
import de.caluga.morphium.annotations.Enumerated;
import de.caluga.morphium.annotations.Id;

/** MOR-093: an entity with enums, for the setting quarkus.morphium.enum-type */
@Entity(collectionName = "it_ranked")
public class RankedEntity {

    public enum Rank { LOW, MID, HIGH }

    @Id
    public String id;

    public Rank plain;

    @Enumerated(EnumType.STRING)
    public Rank named;
}
