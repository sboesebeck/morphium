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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent;
import de.caluga.morphium.quarkus.transaction.MorphiumTransactionEvent.Phase;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.*;
import org.testcontainers.DockerClientFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for {@code @MorphiumTransactional} interceptor and
 * transaction lifecycle events.
 *
 * <p>Transactions require a MongoDB replica set. This test uses Dev Services
 * with {@code quarkus.morphium.devservices.replica-set=true} to start a
 * single-node replica set via Testcontainers.
 *
 * <p>A {@code @BeforeAll} assumption checks {@code DockerClientFactory.instance()
 * .isDockerAvailable()} directly and skips the whole class — with a clear message
 * — when no Docker daemon is reachable, instead of failing the whole
 * {@code integration-tests} build. See D3 ("Begleitmaßnahmen", Punkt 3): the core
 * build must never require Docker.
 *
 * <p><strong>Deliberately not using {@code testcontainers-junit-jupiter}'s
 * {@code @EnabledIfDockerAvailable}</strong>: under Quarkus's test classloading,
 * that annotation's {@code DockerAvailableDetector} reported Docker as unavailable
 * and skipped every test even while Dev Services had already started a real
 * MongoDB container in the same JVM (confirmed via the surefire report showing a
 * successfully started container immediately before the "Docker is not available"
 * skip). Calling {@code DockerClientFactory.instance().isDockerAvailable()}
 * directly — the same class Dev Services itself uses — avoids that discrepancy.
 */
@QuarkusTest
@TestProfile(MorphiumTransactionalTest.ReplicaSetProfile.class)
@DisplayName("@MorphiumTransactional interceptor + events")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumTransactionalTest {

    @BeforeAll
    static void requireDocker() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker is not available — skipping tests that require a MongoDB replica set via Dev Services");
    }

    public static class ReplicaSetProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.morphium.database", "tx-test",
                    "quarkus.morphium.driver-name", "PooledDriver",
                    "quarkus.morphium.devservices.enabled", "true",
                    "quarkus.morphium.devservices.replica-set", "true"
            );
        }
    }

    @Inject
    Morphium morphium;

    @Inject
    TransactionalService service;

    @Inject
    TransactionEventCollector eventCollector;

    @BeforeEach
    void clearEvents() {
        eventCollector.clear();
    }

    @Test
    @Order(0)
    @DisplayName("Dev Services actually started a container: hosts is a container port, driver reports replicaSet=true")
    void devServicesStartedARealContainer() {
        // This is the one thing no other Dev Services test in this suite actually proves:
        // MorphiumDevServicesReplicaSetConfigTest explicitly documents that it starts no
        // container at all (only checks the config keys are bound), and this class's own
        // other tests only prove transactions work -- which happens to require a replica set,
        // but doesn't directly show a container was started for it. Verified here instead:
        // hosts must be a real container-assigned port (Testcontainers never binds to 27017
        // itself), and the driver must report isReplicaSet()==true, which only a real
        // MongoDB replica set negotiates during the driver handshake (an unconfigured
        // standalone mongod would report false).
        String hosts = ConfigProvider.getConfig().getValue("quarkus.morphium.hosts", String.class);
        assertThat(hosts).as("hosts must be injected by Dev Services, not left at the @WithDefault")
                .isNotEqualTo("localhost:27017");
        int port = Integer.parseInt(hosts.substring(hosts.indexOf(':') + 1));
        assertThat(port).as("Dev Services assigns a random container port, not the standard 27017")
                .isNotEqualTo(27017);

        assertThat(morphium.getDriver().isReplicaSet())
                .as("driver must have negotiated replica-set mode with the real container")
                .isTrue();
    }

    @Test
    @Order(1)
    @DisplayName("commit on success – entity is persisted")
    void commit_onSuccess() {
        var item = new ItemEntity();
        item.setName("tx-success");
        item.setPrice(42.0);

        service.storeSuccessfully(item);

        ItemEntity found = morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("tx-success")
                .get();
        assertThat(found).isNotNull();
        assertThat(found.getPrice()).isEqualTo(42.0);
    }

    @Test
    @Order(2)
    @DisplayName("rollback on exception – entity is NOT persisted")
    void rollback_onException() {
        var item = new ItemEntity();
        item.setName("tx-fail");
        item.setPrice(99.0);

        assertThatThrownBy(() -> service.storeAndFail(item))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("forced rollback");

        ItemEntity found = morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("tx-fail")
                .get();
        assertThat(found).isNull();
    }

    @Test
    @Order(3)
    @DisplayName("BEFORE_COMMIT + AFTER_COMMIT events fired on success")
    void events_firedOnCommit() {
        var item = new ItemEntity();
        item.setName("tx-events-commit");

        service.storeSuccessfully(item);

        assertThat(eventCollector.getEvents())
                .extracting(MorphiumTransactionEvent::getPhase)
                .containsExactly(Phase.BEFORE_COMMIT, Phase.AFTER_COMMIT);

        assertThat(eventCollector.getEvents())
                .allSatisfy(e -> assertThat(e.getFailure()).isNull());
    }

    @Test
    @Order(4)
    @DisplayName("AFTER_ROLLBACK event fired on exception, with failure")
    void events_firedOnRollback() {
        var item = new ItemEntity();
        item.setName("tx-events-rollback");

        assertThatThrownBy(() -> service.storeAndFail(item))
                .isInstanceOf(RuntimeException.class);

        assertThat(eventCollector.getEvents())
                .extracting(MorphiumTransactionEvent::getPhase)
                .containsExactly(Phase.AFTER_ROLLBACK);

        assertThat(eventCollector.getEvents().get(0).getFailure())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("forced rollback");
    }
}
