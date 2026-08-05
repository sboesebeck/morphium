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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Morphium's {@code @Embedded} document support.
 * Verifies that nested sub-documents are stored and retrieved correctly.
 */
@QuarkusTest
@DisplayName("@Embedded document support")
class MorphiumEmbeddedTest {

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(CustomerEntity.class);
        morphium.ensureIndicesFor(CustomerEntity.class);
    }

    @Test
    @DisplayName("store/retrieve entity with fully populated embedded address")
    void roundtrip_withEmbeddedAddress() {
        var customer = customerWith("Alice", "Elm St 42", "Springfield", "12345");
        morphium.store(customer);
        assertThat(customer.getId()).isNotNull();

        var found = byName("Alice");
        assertThat(found).isNotNull();
        assertThat(found.getAddress()).isNotNull()
                .satisfies(a -> {
                    assertThat(a.getStreet()).isEqualTo("Elm St 42");
                    assertThat(a.getCity()).isEqualTo("Springfield");
                    assertThat(a.getZip()).isEqualTo("12345");
                });
    }

    @Test
    @DisplayName("store/retrieve entity with null embedded address")
    void roundtrip_withNullAddress() {
        var customer = new CustomerEntity();
        customer.setName("Bob");
        customer.setAddress(null);
        morphium.store(customer);

        var found = byName("Bob");
        assertThat(found).isNotNull();
        assertThat(found.getAddress()).isNull();
    }

    @Test
    @DisplayName("embedded address can be replaced on update")
    void update_replacesEmbeddedAddress() {
        var customer = customerWith("Carol", "Old Lane 1", "Old Town", "00000");
        morphium.store(customer);

        customer.setAddress(address("New Ave 7", "New Town", "99999"));
        morphium.store(customer);

        var found = byName("Carol");
        assertThat(found.getAddress())
                .satisfies(a -> {
                    assertThat(a.getStreet()).isEqualTo("New Ave 7");
                    assertThat(a.getCity()).isEqualTo("New Town");
                    assertThat(a.getZip()).isEqualTo("99999");
                });
    }

    @Test
    @DisplayName("embedded address can be set to null on update")
    void update_clearsEmbeddedAddress() {
        var customer = customerWith("Dave", "Some St", "Somewhere", "11111");
        morphium.store(customer);

        customer.setAddress(null);
        morphium.store(customer);

        var found = byName("Dave");
        assertThat(found.getAddress()).isNull();
    }

    @Test
    @DisplayName("multiple entities with different embedded addresses are independent")
    void multipleEntities_embeddedAddressesAreIndependent() {
        morphium.store(customerWith("Eve", "Eve St", "Eve City", "10000"));
        morphium.store(customerWith("Frank", "Frank Ave", "Frank City", "20000"));

        assertThat(byName("Eve").getAddress().getCity()).isEqualTo("Eve City");
        assertThat(byName("Frank").getAddress().getCity()).isEqualTo("Frank City");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private CustomerEntity customerWith(String name, String street, String city, String zip) {
        var c = new CustomerEntity();
        c.setName(name);
        c.setAddress(address(street, city, zip));
        return c;
    }

    private AddressEmbedded address(String street, String city, String zip) {
        var a = new AddressEmbedded();
        a.setStreet(street);
        a.setCity(city);
        a.setZip(zip);
        return a;
    }

    private CustomerEntity byName(String name) {
        return morphium.createQueryFor(CustomerEntity.class)
                .f("name").eq(name).get();
    }
}
