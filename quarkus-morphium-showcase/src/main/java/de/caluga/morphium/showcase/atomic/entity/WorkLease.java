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
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.Instant;

/** The document of a lease that {@code LeaseManager} keeps: who holds the lease with the id, and until when. */
@Entity(collectionName = "atomic_leases")
@Data
@NoArgsConstructor
@FieldNameConstants
public class WorkLease {

    @Id
    private String id;

    private String ownerName;

    private Instant expiresAt;
}
