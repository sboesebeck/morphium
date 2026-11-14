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
package de.caluga.morphium.showcase.atomic.entity;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.LastChange;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.Instant;

/**
 * A unit of work that several workers compete for (the "Atomic Updates" page).
 *
 * <ul>
 *   <li>{@code code} has a unique index: an update that makes two tickets share a code is a rejected write, which raises since 7.0.</li>
 *   <li>{@code state} is an enum, stored by name; {@code startedAt} is an {@link Instant}, stored as a BSON date. Both are mapped by the update builder like {@code store()} maps them.</li>
 *   <li>{@code changedAt} is a {@code @LastChange} that is declared on the field only: the update builder and the writer methods set it, too.</li>
 * </ul>
 */
@Entity(collectionName = "atomic_tickets")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class JobTicket {

    @Id
    private String id;

    @Index(options = {"unique:1"})
    private String code;

    private JobState state;

    private String owner;

    private int attempts;

    private Instant startedAt;

    @LastChange
    private long changedAt;
}
