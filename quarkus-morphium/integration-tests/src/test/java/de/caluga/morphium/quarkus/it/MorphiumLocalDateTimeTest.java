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
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code LocalDateTime} storage and retrieval.
 *
 * <p>Verifies that the configured {@code LocalDateTimeMapper} (BSON ISODate by default)
 * correctly round-trips Java {@link LocalDateTime} values through the InMemDriver.
 */
@QuarkusTest
@DisplayName("LocalDateTime storage and retrieval")
class MorphiumLocalDateTimeTest {

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(OrderEntity.class);
        morphium.ensureIndicesFor(OrderEntity.class);
    }

    @Test
    @DisplayName("explicit LocalDateTime round-trips correctly")
    void roundtrip_explicitValue() {
        var timestamp = LocalDateTime.of(2024, 6, 15, 10, 30, 45);

        var order = order("ldt-roundtrip", timestamp);
        morphium.store(order);

        var found = byCustomer("ldt-roundtrip");
        assertThat(found).isNotNull();
        assertThat(found.getCreatedAt()).isEqualTo(timestamp);
    }

    @Test
    @DisplayName("@PreStore sets createdAt when null")
    void preStore_setsCreatedAtWhenNull() {
        var order = order("ldt-prestoredt", null);
        assertThat(order.getCreatedAt()).isNull();

        morphium.store(order);

        assertThat(order.getCreatedAt())
                .as("@PreStore must have assigned createdAt")
                .isNotNull();
    }

    @Test
    @DisplayName("createdAt assigned by @PreStore survives the round-trip")
    void preStore_createdAt_survivesRoundtrip() {
        var order = order("ldt-prestoredt-rt", null);
        morphium.store(order);

        LocalDateTime storedAt = order.getCreatedAt();
        assertThat(storedAt).isNotNull();

        var found = byCustomer("ldt-prestoredt-rt");
        assertThat(found.getCreatedAt())
                .as("createdAt from @PreStore must be preserved in the store")
                .isNotNull()
                .isEqualTo(storedAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
    }

    @Test
    @DisplayName("date and time components are preserved")
    void componentsPreserved() {
        var timestamp = LocalDateTime.of(2024, 3, 14, 15, 9, 26);

        morphium.store(order("ldt-components", timestamp));

        var found = byCustomer("ldt-components");
        assertThat(found.getCreatedAt())
                .hasYear(2024)
                .hasMonth(java.time.Month.MARCH)
                .hasDayOfMonth(14)
                .hasHour(15)
                .hasMinute(9)
                .hasSecond(26);
    }

    @Test
    @DisplayName("midnight (00:00:00) is stored and retrieved correctly")
    void midnight_roundtrip() {
        var midnight = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

        morphium.store(order("ldt-midnight", midnight));

        var found = byCustomer("ldt-midnight");
        assertThat(found.getCreatedAt()).isEqualTo(midnight);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private OrderEntity order(String customerId, LocalDateTime createdAt) {
        var o = new OrderEntity();
        o.setCustomerId(customerId);
        o.setAmount(1.0);
        o.setStatus("OPEN");
        o.setCreatedAt(createdAt);
        return o;
    }

    private OrderEntity byCustomer(String customerId) {
        return morphium.createQueryFor(OrderEntity.class)
                .f("customer_id").eq(customerId)
                .get();
    }
}
