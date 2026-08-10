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

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Build-time configuration for Morphium Dev Services.
 *
 * <p>Dev Services automatically start a MongoDB container in dev and test mode
 * when no explicit {@code quarkus.morphium.hosts} is configured.
 *
 * <p>Example – disable Dev Services (use an external MongoDB instead):
 * <pre>{@code
 * quarkus.morphium.devservices.enabled=false
 * quarkus.morphium.hosts=my-mongo:27017
 * quarkus.morphium.database=mydb
 * }</pre>
 */
@ConfigMapping(prefix = "quarkus.morphium.devservices")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface MorphiumDevServicesBuildTimeConfig {

    /**
     * Whether Dev Services are enabled.
     * Set to {@code false} to use an external MongoDB and suppress container startup.
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * Docker image name for the MongoDB container.
     * Defaults to {@code mongo:8} (latest MongoDB 8.x).
     */
    @WithDefault("mongo:8")
    String imageName();

    /**
     * Database name injected as {@code quarkus.morphium.database} when Dev Services start.
     * Override in {@code application.properties} if a different name is needed.
     */
    @WithDefault("morphium-dev")
    String databaseName();

    /**
     * Whether to start MongoDB as a single-node replica set instead of a standalone instance.
     *
     * <p>Defaults to {@code true} so that multi-document transactions, change streams,
     * and other oplog-dependent features work out of the box. The extension achieves this
     * by calling Testcontainers' {@code MongoDBContainer.withReplicaSet()}.
     *
     * <p>Set to {@code false} only if you explicitly need a standalone MongoDB (e.g. with
     * an older image like {@code mongo:6}).
     */
    @WithDefault("true")
    boolean replicaSet();
}
