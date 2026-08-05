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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationRunner;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.runtime.annotations.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Quarkus {@link Recorder} for the Morphium extension.
 *
 * <p>Stores the list of {@code @Entity} and {@code @Embedded} class names
 * discovered at build time so that {@link MorphiumProducer} can clear caches
 * and pre-register them via {@code AnnotationAndReflectionHelper.registerTypeIds()}
 * when the {@code Morphium} instance is created. This skips the ClassGraph scan
 * at runtime and handles dev-mode hot-reload.
 *
 * <p>Also stores {@code @MorphiumChangeUnit} class names and triggers migration
 * execution at runtime when {@code quarkus.morphium.migration.migrate-at-start=true}.
 */
@Recorder
public class MorphiumRecorder {

    private static final Logger log = LoggerFactory.getLogger(MorphiumRecorder.class);

    private static volatile List<String> mappedClassNames = Collections.emptyList();
    private static volatile List<String> migrationClassNames = Collections.emptyList();
    private static volatile List<String> driverClassNames = Collections.emptyList();
    private static volatile List<String> messagingClassNames = Collections.emptyList();
    private static volatile List<String> cappedClassNames = Collections.emptyList();
    private static volatile List<String> entityClassNames = Collections.emptyList();
    private static volatile List<String> embeddedClassNames = Collections.emptyList();

    public void setMappedClassNames(List<String> classNames) {
        mappedClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
    }

    public void setDriverClassNames(List<String> classNames) {
        driverClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
        if (!driverClassNames.isEmpty()) {
            log.debug("Registered {} @Driver classes for native-image pre-population", driverClassNames.size());
        }
    }

    public void setMessagingClassNames(List<String> classNames) {
        messagingClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
        if (!messagingClassNames.isEmpty()) {
            log.debug("Registered {} @Messaging classes for native-image pre-population", messagingClassNames.size());
        }
    }

    public void setCappedClassNames(List<String> classNames) {
        cappedClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
        log.debug("Registered {} @Capped classes for native-image pre-population (empty list prevents ClassGraph scan)", cappedClassNames.size());
    }

    public void setEntityClassNames(List<String> classNames) {
        entityClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
        log.debug("Registered {} @Entity classes for native-image ClassGraphCache pre-population", entityClassNames.size());
    }

    public void setEmbeddedClassNames(List<String> classNames) {
        embeddedClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
        log.debug("Registered {} @Embedded classes for native-image ClassGraphCache pre-population", embeddedClassNames.size());
    }

    public void setMigrationClassNames(List<String> classNames) {
        migrationClassNames = classNames == null ? Collections.emptyList() : List.copyOf(classNames);
        if (!migrationClassNames.isEmpty()) {
            log.debug("Registered {} @MorphiumChangeUnit migration classes", migrationClassNames.size());
        }
    }

    /**
     * Called at RUNTIME_INIT after the BeanContainer is available.
     * Triggers migration execution if configured.
     */
    public void runMigrations() {
        // Resolve config first to check migrateAtStart before triggering Morphium initialization
        try (InstanceHandle<MorphiumRuntimeConfig> configHandle = Arc.container().instance(MorphiumRuntimeConfig.class)) {
            MorphiumRuntimeConfig config = configHandle.get();
            if (config == null) {
                throw new IllegalStateException("MorphiumRuntimeConfig not available — cannot run migrations");
            }

            if (!config.migration().migrateAtStart()) {
                log.debug("quarkus.morphium.migration.migrate-at-start=false — skipping migrations");
                return;
            }

            if (migrationClassNames.isEmpty()) {
                log.info("No @MorphiumChangeUnit classes discovered — nothing to migrate");
                return;
            }

            // Only resolve Morphium (triggering DB connection) when migrations are actually needed
            try (InstanceHandle<Morphium> morphiumHandle = Arc.container().instance(Morphium.class)) {
                Morphium morphium = morphiumHandle.get();
                if (morphium == null) {
                    throw new IllegalStateException("Morphium bean not available — cannot run migrations");
                }

                log.info("Running {} database migration(s) at startup", migrationClassNames.size());
                MorphiumMigrationRunner runner = new MorphiumMigrationRunner(morphium, config.migration());
                runner.execute(migrationClassNames);
            }
        }
    }

    static List<String> getMappedClassNames() {
        return mappedClassNames;
    }

    static List<String> getMigrationClassNames() {
        return migrationClassNames;
    }

    static List<String> getDriverClassNames() {
        return driverClassNames;
    }

    static List<String> getMessagingClassNames() {
        return messagingClassNames;
    }

    static List<String> getCappedClassNames() {
        return cappedClassNames;
    }

    static List<String> getEntityClassNames() {
        return entityClassNames;
    }

    static List<String> getEmbeddedClassNames() {
        return embeddedClassNames;
    }
}
