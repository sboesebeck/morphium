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
package de.caluga.morphium.quarkus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that the application starts successfully when
 * {@code quarkus.morphium.devservices.replica-set=true} is set alongside
 * {@code devservices.enabled=false}.
 *
 * <p>No MongoDB container is started. This test only proves that the config
 * overrides are present in MicroProfile Config and that startup completes
 * without errors — it does <em>not</em> prove that Quarkus treats the key as
 * a recognised {@code @ConfigMapping} property (MicroProfile Config returns
 * arbitrary keys from any config source). The actual {@code @ConfigMapping}
 * binding (property-name → {@code replicaSet()} method) is covered by
 * {@code MorphiumDevServicesConfigDefaultsTest} in the deployment module.
 */
@QuarkusTest
@TestProfile(MorphiumDevServicesReplicaSetConfigTest.ReplicaSetEnabledProfile.class)
@DisplayName("Dev Services – startup with replica-set override (no container)")
class MorphiumDevServicesReplicaSetConfigTest {

    /**
     * Test profile that enables the {@code replica-set} flag while keeping
     * Dev Services disabled so no Docker container is started.
     */
    public static class ReplicaSetEnabledProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.morphium.driver-name",             "InMemDriver",
                    "quarkus.morphium.database",                "replset-cfg-test",
                    "quarkus.morphium.devservices.enabled",     "false",
                    "quarkus.morphium.devservices.replica-set", "true"
            );
        }
    }

    @Test
    @DisplayName("replica-set=true with devservices.enabled=false does not prevent startup")
    void replicaSet_withDevServicesDisabled_appStartsSuccessfully() {
        // Reaching this point means the Quarkus application started without errors
        // despite replica-set=true being set. We assert both overrides are present
        // in MicroProfile Config. Note: this does NOT prove Quarkus treats them as
        // recognised @ConfigMapping properties — MicroProfile Config returns any key
        // from any config source. The actual binding is tested by
        // MorphiumDevServicesConfigDefaultsTest in the deployment module.
        assertThat(ConfigProvider.getConfig()
                .getValue("quarkus.morphium.devservices.enabled", String.class))
                .isEqualTo("false");
        assertThat(ConfigProvider.getConfig()
                .getValue("quarkus.morphium.devservices.replica-set", String.class))
                .as("replica-set profile override must be present")
                .isEqualTo("true");
    }

    @Test
    @DisplayName("driver is InMemDriver (no MongoDB connection needed)")
    void driver_isInMemory() {
        assertThat(ConfigProvider.getConfig()
                .getValue("quarkus.morphium.driver-name", String.class))
                .isEqualTo("InMemDriver");
    }
}
