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
package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.quarkus.MorphiumRecorder;
import de.caluga.morphium.quarkus.migration.MorphiumChangeUnit;
import io.quarkus.arc.deployment.SyntheticBeansRuntimeInitBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.ServiceStartBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Build-time processor for the Morphium migration framework.
 *
 * <p>Scans the Jandex index for {@link MorphiumChangeUnit} annotated classes,
 * registers them for GraalVM reflection, passes them to the {@link MorphiumRecorder},
 * and triggers migration execution at runtime.
 */
public class MorphiumMigrationProcessor {

    private static final Logger log = Logger.getLogger(MorphiumMigrationProcessor.class);
    private static final DotName CHANGE_UNIT = DotName.createSimple(MorphiumChangeUnit.class.getName());

    /**
     * Discovers all {@code @MorphiumChangeUnit} classes at build time and passes
     * their names to the recorder for runtime execution.
     */
    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    void discoverMigrations(CombinedIndexBuildItem combinedIndex,
                            BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                            MorphiumRecorder recorder) {

        IndexView index = combinedIndex.getIndex();
        List<String> migrationClassNames = new ArrayList<>();

        for (AnnotationInstance ai : index.getAnnotations(CHANGE_UNIT)) {
            if (ai.target().kind() != AnnotationTarget.Kind.CLASS) {
                continue;
            }
            String className = ai.target().asClass().name().toString();
            migrationClassNames.add(className);

            // Register for GraalVM native image reflection
            reflectiveClasses.produce(ReflectiveClassBuildItem.builder(className)
                    .constructors(true)
                    .methods(true)
                    .fields(true)
                    .build());

            log.debugf("Morphium Migration: discovered @MorphiumChangeUnit %s", className);
        }

        if (!migrationClassNames.isEmpty()) {
            log.infof("Morphium Migration: discovered %d @MorphiumChangeUnit class(es)", migrationClassNames.size());
        }

        recorder.setMigrationClassNames(migrationClassNames);
    }

    /**
     * Executes pending migrations at RUNTIME_INIT after the Morphium bean is available.
     * Consumes {@link MorphiumEntitiesRegisteredBuildItem} to guarantee that
     * {@code setMappedClassNames()} has been replayed before this step runs —
     * otherwise the Morphium bean creation triggered here would see an empty
     * entity list and skip index creation.
     * Produces a {@link ServiceStartBuildItem} to ensure migrations complete before
     * the application starts serving requests.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    @Consume(SyntheticBeansRuntimeInitBuildItem.class)
    ServiceStartBuildItem executeMigrations(MorphiumRecorder recorder,
                                            MorphiumEntitiesRegisteredBuildItem entitiesRegistered) {
        recorder.runMigrations();
        return new ServiceStartBuildItem("morphium-migration");
    }
}
