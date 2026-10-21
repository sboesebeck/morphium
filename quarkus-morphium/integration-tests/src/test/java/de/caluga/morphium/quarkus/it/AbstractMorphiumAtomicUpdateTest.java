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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.testing.ResetMorphiumExtension;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Date;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-021: a repository method acquires a lease with one atomic upsert ({@code @AtomicUpdate}). The subclasses run it on the in-memory driver and on a real
 * MongoDB.
 */
@ExtendWith(ResetMorphiumExtension.class)
abstract class AbstractMorphiumAtomicUpdateTest {

    @Inject
    LeaseRepository leases;

    @Inject
    Morphium morphium;

    @Test
    @DisplayName("acquire, lose against the holder, renew, take over after the expiry")
    void leaseLifecycle() {
        Optional<LeaseEntity> first = leases.tryAcquire("job", "A", new Date(1000), new Date(5000));
        assertThat(first).isPresent();
        assertThat(first.get().getOwner()).isEqualTo("A");
        assertThat(first.get().getFencing()).isEqualTo(1);
        assertThat(first.get().getCreatedBy()).isEqualTo("A");

        assertThat(leases.tryAcquire("job", "B", new Date(2000), new Date(6000))).isEmpty();

        Optional<LeaseEntity> renewed = leases.tryAcquire("job", "A", new Date(3000), new Date(8000));
        assertThat(renewed.get().getFencing()).isEqualTo(2);

        Optional<LeaseEntity> takenOver = leases.tryAcquire("job", "B", new Date(9000), new Date(12000));
        assertThat(takenOver.get().getOwner()).isEqualTo("B");
        assertThat(takenOver.get().getFencing()).isEqualTo(3);
        assertThat(takenOver.get().getCreatedBy()).isEqualTo("A");
        assertThat(morphium.createQueryFor(LeaseEntity.class).countAll()).isEqualTo(1L);
    }

    @Test
    @DisplayName("the count form: true when acquired, false when lost; release returns the matched count")
    void countForm() {
        assertThat(leases.acquired("job", "A", new Date(1000), new Date(5000))).isTrue();
        assertThat(leases.acquired("job", "B", new Date(2000), new Date(6000))).isFalse();
        assertThat(leases.release("job", "B", new Date(0))).isZero();
        assertThat(leases.release("job", "A", new Date(0))).isEqualTo(1);
        assertThat(leases.acquired("job", "B", new Date(2000), new Date(6000))).isTrue();
    }

    @Test
    @DisplayName("the entity as the result, with the state BEFORE; null if nothing matches")
    void entityResult() {
        leases.tryAcquire("job", "A", new Date(1000), new Date(5000));

        LeaseEntity old = leases.bumpAndGetOld("job");

        assertThat(old.getFencing()).isEqualTo(1);
        assertThat(morphium.findById(LeaseEntity.class, "job", null).getFencing()).isEqualTo(2);
        assertThat(leases.bumpAndGetOld("nobody")).isNull();
    }
}
