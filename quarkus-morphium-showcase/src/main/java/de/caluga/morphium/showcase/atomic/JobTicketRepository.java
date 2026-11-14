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
package de.caluga.morphium.showcase.atomic;

import de.caluga.morphium.showcase.atomic.entity.JobState;
import de.caluga.morphium.showcase.atomic.entity.JobTicket;
import jakarta.data.repository.BasicRepository;
import jakarta.data.repository.Param;
import jakarta.data.repository.Repository;

import java.util.Optional;

/**
 * Derived update methods (a Morphium extension of Jakarta Data, since 7.0): the part of the name after {@code updateBy} is the condition and takes the first parameters; every parameter
 * after them is a value to set, and its name is the field it sets. One atomic update command per call.
 */
@Repository
public interface JobTicketRepository extends BasicRepository<JobTicket, String> {

    /** {@code WHERE state = :state}, then {@code $set owner}, for every matching ticket; the count of the matches. */
    long updateByState(JobState state, @Param("owner") String owner);

    /** {@code WHERE id = :id AND state = :state}, then {@code $set owner}; the ticket as it is after the update, empty if it does not match. */
    Optional<JobTicket> updateByIdAndState(String id, JobState state, @Param("owner") String owner);
}
