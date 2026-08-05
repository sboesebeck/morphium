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
package de.caluga.morphium.quarkus.migration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a Morphium database migration unit.
 *
 * <p>Each change unit must contain exactly one method annotated with {@link Execution}
 * and optionally one method annotated with {@link RollbackExecution}.
 *
 * <p>Example:
 * <pre>{@code
 * @MorphiumChangeUnit(id = "001-init-products", order = "001", author = "team")
 * public class InitProductsMigration {
 *
 *     @Execution
 *     public void execute(Morphium morphium) {
 *         morphium.store(new Product("Widget", 9.99));
 *     }
 *
 *     @RollbackExecution
 *     public void rollback(Morphium morphium) {
 *         morphium.dropCollection(Product.class);
 *     }
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MorphiumChangeUnit {

    /** Unique identifier for this migration. Used to track execution state. */
    String id();

    /**
     * Execution order. Migrations are sorted lexicographically by this value.
     * Use zero-padded numbers for predictable ordering (e.g. "001", "002").
     */
    String order();

    /** Author of this migration (informational). */
    String author() default "";
}
