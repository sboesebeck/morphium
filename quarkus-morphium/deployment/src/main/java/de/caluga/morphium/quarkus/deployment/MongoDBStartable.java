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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper around a Testcontainers MongoDB container, managed by
 * {@link MorphiumDevServicesProcessor} via static volatile fields.
 */
class MongoDBStartable {

    private static final int MONGO_PORT = 27017;

    private final String imageName;
    private final boolean replicaSet;
    private GenericContainer<?> container;

    MongoDBStartable(String imageName, boolean replicaSet) {
        this.imageName = imageName;
        this.replicaSet = replicaSet;
    }

    @SuppressWarnings("resource")
    void start() {
        if (container != null) {
            return;
        }
        DockerImageName base = DockerImageName.parse(imageName);
        DockerImageName substituted = ImageNameSubstitutor.instance().apply(base)
                .asCompatibleSubstituteFor("mongo");

        if (replicaSet) {
            container = new MongoDBContainer(substituted).withReplicaSet();
        } else {
            container = new GenericContainer<>(substituted)
                    .withExposedPorts(MONGO_PORT)
                    .waitingFor(Wait.forLogMessage(".*Waiting for connections.*\n", 1));
        }
        container.start();
    }

    void close() {
        if (container != null && container.isRunning()) {
            container.stop();
        }
    }

    String getHost() {
        ensureStarted();
        return container.getHost();
    }

    String getContainerId() {
        return container != null ? container.getContainerId() : null;
    }

    int getMappedPort() {
        ensureStarted();
        return container.getMappedPort(MONGO_PORT);
    }

    boolean isReplicaSet() {
        return replicaSet;
    }

    String getReplicaSetName() {
        if (container instanceof MongoDBContainer mongoContainer) {
            String connStr = mongoContainer.getConnectionString();
            Matcher m = Pattern.compile("[?&]replicaSet=([^&]+)")
                    .matcher(connStr);
            return m.find() ? m.group(1) : "docker-rs";
        }
        return null;
    }

    private void ensureStarted() {
        if (container == null) {
            throw new IllegalStateException("MongoDBStartable has not been started yet");
        }
    }
}
