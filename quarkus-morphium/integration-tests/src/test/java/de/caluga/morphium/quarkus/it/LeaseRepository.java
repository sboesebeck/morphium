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

import de.caluga.morphium.data.Assign;
import de.caluga.morphium.data.AtomicUpdate;
import jakarta.data.repository.BasicRepository;
import jakarta.data.repository.Param;
import jakarta.data.repository.Repository;

import java.util.Date;
import java.util.Optional;

/** MOR-021: a lease acquired by ONE atomic upsert, without touching the driver API. */
@Repository
public interface LeaseRepository extends BasicRepository<LeaseEntity, String> {

    /** acquires, renews (same owner) or takes over (expired); empty if somebody else holds it */
    @AtomicUpdate(where = "id = :id AND (expiresAt < :now OR owner = :owner)",
                  set = {@Assign(field = "owner", param = "owner"), @Assign(field = "expiresAt", param = "until")},
                  setOnInsert = @Assign(field = "createdBy", param = "owner"),
                  inc = @Assign(field = "fencing", value = "1"),
                  upsert = true)
    Optional<LeaseEntity> tryAcquire(@Param("id") String id, @Param("owner") String owner, @Param("now") Date now, @Param("until") Date until);

    /** the same, as the count form: true if it was acquired */
    @AtomicUpdate(where = "id = :id AND (expiresAt < :now OR owner = :owner)",
                  set = {@Assign(field = "owner", param = "owner"), @Assign(field = "expiresAt", param = "until")},
                  inc = @Assign(field = "fencing", value = "1"),
                  upsert = true)
    boolean acquired(@Param("id") String id, @Param("owner") String owner, @Param("now") Date now, @Param("until") Date until);

    /** releases a lease of the owner: the count form without upsert, int result */
    @AtomicUpdate(where = "id = :id AND owner = :owner", set = @Assign(field = "expiresAt", param = "expired"))
    int release(@Param("id") String id, @Param("owner") String owner, @Param("expired") Date expired);

    /** the entity itself as the result of the find-and-modify form; the state BEFORE */
    @AtomicUpdate(where = "id = :id", inc = @Assign(field = "fencing", value = "1"), returns = de.caluga.morphium.query.ReturnDocument.BEFORE)
    LeaseEntity bumpAndGetOld(@Param("id") String id);

    /** the same without @Param: the names of the parameters are kept (-parameters, the default of the build under JDK 25 with the option set) */
    @AtomicUpdate(where = "id = :id AND (expiresAt < :now OR owner = :owner)",
                  set = {@Assign(field = "owner", param = "owner"), @Assign(field = "expiresAt", param = "until")},
                  inc = @Assign(field = "fencing", value = "1"),
                  upsert = true)
    Optional<LeaseEntity> acquireByParameterNames(String id, String owner, Date now, Date until);
}
