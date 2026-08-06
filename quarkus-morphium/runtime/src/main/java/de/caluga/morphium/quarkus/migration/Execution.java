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
 * Marks a method inside a {@link MorphiumChangeUnit} as the migration execution method.
 *
 * <p>The method may accept a single {@link de.caluga.morphium.Morphium} parameter
 * or no parameters at all.
 *
 * <p>Each {@link MorphiumChangeUnit} must have exactly one {@code @Execution} method.
 *
 * <p><b>Must be idempotent.</b> The changelog entry marking a change unit as executed is written
 * only <em>after</em> this method returns successfully. If the process crashes (or is killed)
 * between this method completing its work and that changelog write, the next run sees no
 * changelog entry for this change unit and executes it again — the method's own effects (e.g.
 * an insert that already succeeded once) must survive being applied a second time without
 * corrupting data or throwing. Prefer {@code upsert} over unconditional insert, make deletes
 * conditional on existence, and design any external side effect (a call to another service, a
 * message published, etc.) to tolerate being triggered twice for the same logical migration run.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Execution {
}
