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

import io.quarkus.deployment.IsDevServicesSupportedByLaunchMode;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CuratedApplicationShutdownBuildItem;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.DockerStatusBuildItem;
import io.quarkus.runtime.configuration.ConfigUtils;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Quarkus build-time processor that automatically starts a MongoDB container
 * in dev and test mode when no explicit {@code quarkus.morphium.hosts} is configured.
 *
 * <p>Uses static volatile fields for container reuse across augmentation phases
 * (e.g. different {@code @QuarkusTestProfile} switches). This is the same pattern
 * used by Quarkus's own MongoDB extension ({@code DevServicesMongoProcessor}).
 *
 * <p>The Quarkus {@code owned()} Dev Services API has a container reuse defect:
 * {@code ComparableDevServicesConfig} overrides {@code equals()} with
 * {@code reflectiveEquals()} for cross-classloader comparison but does NOT override
 * {@code hashCode()}. The auto-generated record {@code hashCode()} calls
 * {@code .hashCode()} on the {@code globalConfig} proxy, which is identity-based
 * and differs across augmentation phases. This causes {@code ConcurrentHashMap.get()}
 * to miss the existing entry, creating a new container each augmentation.
 *
 * <p>Dev Services are skipped when:
 * <ul>
 *   <li>{@code quarkus.morphium.devservices.enabled=false}</li>
 *   <li>{@code quarkus.morphium.hosts} is explicitly set</li>
 *   <li>The application runs in normal (production) mode</li>
 * </ul>
 */
public class MorphiumDevServicesProcessor {

    private static final Logger log = Logger.getLogger(MorphiumDevServicesProcessor.class);

    // Static fields survive across augmentation phases because the deployment
    // processor class is loaded once and not replaced during re-augmentation.
    static volatile MongoDBStartable runningContainer;
    static volatile CapturedConfig capturedConfig;
    static volatile boolean first = true;

    @BuildStep(onlyIf = IsDevServicesSupportedByLaunchMode.class)
    public DevServicesResultBuildItem startDevServices(
            MorphiumDevServicesBuildTimeConfig config,
            DockerStatusBuildItem dockerStatusBuildItem,
            CuratedApplicationShutdownBuildItem closeBuildItem) {

        if (!config.enabled()) {
            log.debug("Morphium Dev Services disabled via quarkus.morphium.devservices.enabled=false");
            return null;
        }

        if (!dockerStatusBuildItem.isDockerAvailable()) {
            // Same guard Quarkus's own DevServicesMongoProcessor uses. Without it, a container
            // start attempt on a machine with no Docker daemon throws mid-augmentation and
            // fails the whole build instead of just skipping Dev Services -- the exact
            // scenario the module's own MorphiumTransactionalTest works around at the test
            // level (a @BeforeAll Docker check), but which has no equivalent guard here at
            // the point the container would actually be started.
            log.warn("Docker isn't working, please configure quarkus.morphium.hosts or "
                    + "quarkus.morphium.atlas-url — Morphium Dev Services will not start a MongoDB container");
            return null;
        }

        if (ConfigUtils.isPropertyNonEmpty("quarkus.morphium.hosts")
                || ConfigUtils.isPropertyNonEmpty("quarkus.morphium.atlas-url")) {
            log.debug("Morphium connection settings already configured – skipping Dev Services");
            return null;
        }

        if (ConfigUtils.getFirstOptionalValue(List.of("quarkus.morphium.driver-name"), String.class)
                .map(driverName -> driverName.equalsIgnoreCase("InMemDriver"))
                .orElse(false)) {
            log.debugf("Morphium driver-name explicitly set to InMemDriver – " +
                    "skipping Dev Services since no real MongoDB connection is needed");
            return null;
        }

        CapturedConfig currentConfig = new CapturedConfig(config.imageName(), config.replicaSet(), config.databaseName());

        // Reuse existing container if config hasn't changed
        if (runningContainer != null) {
            if (currentConfig.equals(capturedConfig)) {
                log.debug("Reusing existing MongoDB Dev Services container");
                return buildResult(runningContainer, currentConfig);
            }
            // Config changed — close old container and start fresh
            log.info("Morphium Dev Services config changed — restarting container");
            closeContainer();
        }

        log.infof("Morphium Dev Services: starting MongoDB %s from image '%s'",
                config.replicaSet() ? "replica set" : "standalone", config.imageName());

        MongoDBStartable startable = new MongoDBStartable(config.imageName(), config.replicaSet());
        startable.start();

        runningContainer = startable;
        capturedConfig = currentConfig;

        // Register shutdown hook (only once per JVM lifecycle)
        if (first) {
            first = false;
            closeBuildItem.addCloseTask(() -> {
                closeContainer();
                first = true;
            }, true);
        }

        log.infof("MongoDB Dev Services ready at %s:%d", startable.getHost(), startable.getMappedPort());

        return buildResult(startable, currentConfig);
    }

    private static DevServicesResultBuildItem buildResult(MongoDBStartable startable, CapturedConfig config) {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("quarkus.morphium.hosts", startable.getHost() + ":" + startable.getMappedPort());
        configMap.put("quarkus.morphium.database", config.databaseName());
        if (config.replicaSet()) {
            String rsName = startable.getReplicaSetName();
            configMap.put("quarkus.morphium.replica-set-name", rsName != null ? rsName : "docker-rs");
        }

        return DevServicesResultBuildItem.discovered()
                .feature("morphium")
                .containerId(startable.getContainerId())
                .config(configMap)
                .description("MongoDB (" + config.imageName() + ")")
                .build();
    }

    private static void closeContainer() {
        if (runningContainer != null) {
            try {
                runningContainer.close();
            } catch (Exception e) {
                log.warn("Failed to close MongoDB Dev Services container", e);
            }
            runningContainer = null;
            capturedConfig = null;
        }
    }

    record CapturedConfig(String imageName, boolean replicaSet, String databaseName) {
    }
}
