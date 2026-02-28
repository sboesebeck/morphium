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
package io.quarkiverse.morphium.showcase.polymorphism;

import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import io.quarkiverse.morphium.showcase.polymorphism.entity.Money;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;

/**
 * Custom type mapper that teaches Morphium how to serialize and deserialize {@link Money} objects.
 *
 * <p>Morphium's {@link MorphiumTypeMapper} interface is the extension point for controlling how
 * arbitrary Java types are converted to/from MongoDB BSON representations. When Morphium encounters
 * a field of type {@code Money} during serialization, it delegates to this mapper instead of using
 * the default object mapping.</p>
 *
 * <h3>How custom type mappers work in Morphium</h3>
 * <ol>
 *   <li><strong>Registration:</strong> In Quarkus, annotating this class with {@code @ApplicationScoped}
 *       makes it a CDI bean. The quarkus-morphium extension auto-discovers all
 *       {@code MorphiumTypeMapper} beans and registers them with the Morphium object mapper at startup.</li>
 *   <li><strong>Marshall (Java to MongoDB):</strong> The {@link #marshall(Money)} method converts a
 *       {@code Money} instance into a MongoDB-compatible representation (here, a simple {@code Map}).
 *       Morphium stores this map as an embedded sub-document.</li>
 *   <li><strong>Unmarshall (MongoDB to Java):</strong> The {@link #unmarshall(Object)} method receives
 *       the raw data read from MongoDB (typically a {@code Map}) and reconstructs the Java object.</li>
 * </ol>
 *
 * <h3>MongoDB document structure (example)</h3>
 * A {@code Money} field on an entity would be stored as:
 * <pre>{@code
 * {
 *   "amount": 115000.0,
 *   "currency": "EUR"
 * }
 * }</pre>
 *
 * @see Money
 * @see MorphiumTypeMapper
 */
@ApplicationScoped
public class MoneyMapper implements MorphiumTypeMapper<Money> {

    /**
     * Converts a {@link Money} instance into a MongoDB-compatible map representation.
     *
     * <p>The returned {@code Map} will be stored as an embedded BSON sub-document.
     * Morphium calls this method automatically whenever it encounters a {@code Money}
     * field during entity serialization.</p>
     *
     * @param o the Money value object to serialize
     * @return a Map with "amount" and "currency" keys, suitable for BSON storage
     */
    @Override
    public Object marshall(Money o) {
        // Convert the Money POJO into a simple key-value map.
        // Morphium will store this as an embedded BSON document in MongoDB.
        return Map.of(
                "amount", o.getAmount(),
                "currency", o.getCurrency()
        );
    }

    /**
     * Reconstructs a {@link Money} instance from data read from MongoDB.
     *
     * <p>Morphium calls this method during deserialization when it encounters a field
     * whose registered type mapper is this class. The input {@code d} is typically a
     * {@code Map<String, Object>} representing the embedded BSON sub-document.</p>
     *
     * @param d the raw MongoDB data (expected to be a Map with "amount" and "currency" keys)
     * @return the reconstructed Money instance
     * @throws IllegalArgumentException if the input cannot be interpreted as a Money object
     */
    @Override
    public Money unmarshall(Object d) {
        // The data coming from MongoDB is a Map (BSON document -> Java Map).
        // We extract the fields and construct the Money value object.
        if (d instanceof Map<?, ?> map) {
            double amount = ((Number) map.get("amount")).doubleValue();
            String currency = (String) map.get("currency");
            return new Money(amount, currency);
        }
        throw new IllegalArgumentException("Cannot unmarshall Money from: " + d);
    }
}