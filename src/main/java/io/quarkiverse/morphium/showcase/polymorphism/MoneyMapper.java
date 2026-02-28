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

@ApplicationScoped
public class MoneyMapper implements MorphiumTypeMapper<Money> {

    @Override
    public Object marshall(Money o) {
        return Map.of(
                "amount", o.getAmount(),
                "currency", o.getCurrency()
        );
    }

    @Override
    public Money unmarshall(Object d) {
        if (d instanceof Map<?, ?> map) {
            double amount = ((Number) map.get("amount")).doubleValue();
            String currency = (String) map.get("currency");
            return new Money(amount, currency);
        }
        throw new IllegalArgumentException("Cannot unmarshall Money from: " + d);
    }
}