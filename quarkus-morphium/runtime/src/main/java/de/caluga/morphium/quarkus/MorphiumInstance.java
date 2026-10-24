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
package de.caluga.morphium.quarkus;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Selects a named Morphium instance (MOR-032): {@code @MorphiumInstance("analytics") Morphium analytics;} is the instance that
 * {@code quarkus.morphium.named."analytics".*} configures. A plain {@code @Inject Morphium} is still the default instance.
 */
@Qualifier
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD, ElementType.TYPE})
public @interface MorphiumInstance {

    /** the name in {@code quarkus.morphium.named."name".*} */
    String value();

    /** for programmatic lookup: {@code Instance<Morphium>.select(new MorphiumInstance.Literal("analytics"))} */
    final class Literal extends AnnotationLiteral<MorphiumInstance> implements MorphiumInstance {
        private static final long serialVersionUID = 1L;
        private final String value;

        public Literal(String value) {
            this.value = value;
        }

        @Override
        public String value() {
            return value;
        }
    }
}
