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
     * Execution order, used to sort migrations before running them.
     *
     * <p>Compared numerically when both this and the other migration's {@code order()} value
     * parse as a number (e.g. {@code "2"} sorts before {@code "10"}), falling back to a plain
     * lexicographic string comparison otherwise -- so a non-numeric convention (e.g. date-based
     * order values) is also supported. Zero-padded numbers (e.g. {@code "001"}, {@code "002"})
     * work correctly either way and remain the recommended convention for readability.
     */
    String order();

    /** Author of this migration (informational). */
    String author() default "";
}
