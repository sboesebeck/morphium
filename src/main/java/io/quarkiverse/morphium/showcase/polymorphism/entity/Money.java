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
package io.quarkiverse.morphium.showcase.polymorphism.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A value object representing a monetary amount with currency.
 *
 * <p>This class demonstrates the use of <strong>custom type mappers</strong> in Morphium.
 * {@code Money} is <em>not</em> annotated with {@code @Entity} because it is not a top-level
 * collection entity -- instead, it is intended to be embedded within other entities. However,
 * Morphium does not know how to serialize/deserialize it by default.</p>
 *
 * <p>To teach Morphium how to handle this class, a custom {@link de.caluga.morphium.objectmapping.MorphiumTypeMapper}
 * is registered: {@link io.quarkiverse.morphium.showcase.polymorphism.MoneyMapper}. That mapper
 * converts {@code Money} to/from a plain {@code Map} for MongoDB storage.</p>
 *
 * <h3>When to use a custom type mapper</h3>
 * <ul>
 *   <li>Value objects that you want to control the serialization format for</li>
 *   <li>Third-party classes that you cannot annotate with {@code @Entity} or {@code @Embedded}</li>
 *   <li>Types that require special conversion logic (e.g. encoding a complex type into a simpler structure)</li>
 * </ul>
 *
 * @see io.quarkiverse.morphium.showcase.polymorphism.MoneyMapper
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Money {

    /** The numeric monetary amount (e.g. 115000.00). */
    private double amount;

    /** The ISO 4217 currency code (e.g. "EUR", "USD"). */
    private String currency;
}