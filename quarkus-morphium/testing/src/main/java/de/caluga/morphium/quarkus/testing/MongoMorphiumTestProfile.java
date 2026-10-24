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
package de.caluga.morphium.quarkus.testing;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

/**
 * Quarkus test profile that runs a test against a real MongoDB with the pooled driver.
 *
 * <pre>{@code
 * @QuarkusTest
 * @TestProfile(MongoMorphiumTestProfile.class)
 * @ExtendWith(ResetMorphiumExtension.class)
 * class MyRepositoryTest { ... }
 * }</pre>
 *
 * <p>Two ways to get the server:
 * <ul>
 *   <li><b>Container</b> (default): Dev Services starts a {@code mongo} container with Testcontainers.
 *       Docker has to be available.</li>
 *   <li><b>Existing server</b>: set the environment variable (or system property)
 *       {@code MORPHIUM_TEST_HOSTS}, for example {@code localhost:27017} or {@code mongo1:27017,mongo2:27017}.
 *       Dev Services is switched off then. The tests use their own database ({@link #database()}), never the
 *       database of the application, and {@link MorphiumTestSupport#reset} empties all of its collections.</li>
 * </ul>
 *
 * <p>Override {@link #database()} or {@link #replicaSet()} in a subclass. A transaction needs a replica set.
 */
public class MongoMorphiumTestProfile implements QuarkusTestProfile {

    /** The name of the environment variable and system property with the hosts of an existing server. */
    public static final String HOSTS_PROPERTY = "MORPHIUM_TEST_HOSTS";

    /** The database that the test uses. */
    protected String database() {
        return "morphium-test";
    }

    /** Whether the container is started as a single-node replica set; ignored for an existing server. */
    protected boolean replicaSet() {
        return false;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("quarkus.morphium.driver-name", "PooledDriver");
        cfg.put("quarkus.morphium.database", database());
        String hosts = System.getProperty(HOSTS_PROPERTY, System.getenv(HOSTS_PROPERTY));

        if (hosts != null && !hosts.isBlank()) {
            cfg.put("quarkus.morphium.hosts", hosts.trim());
            cfg.put("quarkus.morphium.devservices.enabled", "false");
        } else {
            cfg.put("quarkus.morphium.devservices.enabled", "true");
            cfg.put("quarkus.morphium.devservices.replica-set", Boolean.toString(replicaSet()));
        }

        return cfg;
    }
}
