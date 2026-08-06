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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.DockerClientFactory;

/**
 * JUnit 5 {@link ExecutionCondition} that disables a test class when no Docker daemon is
 * reachable, evaluated <em>before</em> {@code QuarkusTestExtension} boots the application.
 *
 * <p><b>Why this exists instead of a {@code @BeforeAll} assumption:</b> {@code @QuarkusTest}
 * boots the Quarkus application (including Dev Services) as part of
 * {@code QuarkusTestExtension}'s {@code beforeAll} callback, which JUnit invokes strictly
 * <em>before</em> the test class's own {@code @BeforeAll} methods. By the time a
 * {@code @BeforeAll}-based {@code assumeTrue(...)} check would run, the application has
 * already tried (and, without Docker, failed) to boot — the assumption never gets a chance
 * to skip anything. An {@link ExecutionCondition} registered via {@code @ExtendWith}
 * participates in JUnit's {@code shouldBeStopped}/container-execution evaluation, which runs
 * ahead of any extension's own {@code beforeAll}, including {@code QuarkusTestExtension}'s —
 * so disabling here actually prevents the boot attempt.
 *
 * <p><b>Why not {@code testcontainers-junit-jupiter}'s {@code @EnabledIfDockerAvailable}</b>:
 * see the class-level Javadoc on {@link MorphiumTransactionalTest} — under Quarkus's test
 * classloading, that annotation's detector reported Docker as unavailable even while Dev
 * Services had already started a real container in the same JVM. Calling
 * {@code DockerClientFactory.instance().isDockerAvailable()} directly — the same class Dev
 * Services itself uses — avoids that discrepancy.
 */
public class DockerAvailableCondition implements ExecutionCondition {

    private static final ConditionEvaluationResult DOCKER_AVAILABLE =
            ConditionEvaluationResult.enabled("Docker is available");

    private static final ConditionEvaluationResult DOCKER_NOT_AVAILABLE =
            ConditionEvaluationResult.disabled(
                    "Docker is not available — skipping tests that require a MongoDB replica set via Dev Services");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        // Only perform the actual Docker check at the container (class) level, i.e. before
        // QuarkusTestExtension boots the app and swaps in its own test classloader. Once the
        // class-level check has enabled the container, JUnit re-evaluates all registered
        // ExecutionConditions again for each individual test method; the class-level result
        // already determined whether the whole container should run, so per-method
        // evaluations simply trust that decision instead of repeating the check.
        if (context.getTestMethod().isPresent()) {
            return DOCKER_AVAILABLE;
        }
        return isDockerAvailable() ? DOCKER_AVAILABLE : DOCKER_NOT_AVAILABLE;
    }

    /**
     * Calls {@code DockerClientFactory.instance().isDockerAvailable()} with the current
     * thread's context classloader temporarily forced to this class's own defining
     * classloader.
     *
     * <p>Without this, {@code isDockerAvailable()} fails with a hard
     * {@code ServiceConfigurationError} ("... not a subtype") instead of returning a clean
     * {@code true}/{@code false} when the JUnit Platform Launcher's forked JVM (Surefire,
     * {@code reuseForks=true} by default) has already run an earlier {@code @QuarkusTest}
     * class in the same fork: Quarkus's own {@code QuarkusClassLoader} for that earlier class
     * can be left installed as the thread's context classloader, and {@code DockerClientFactory}
     * internally does a plain {@code ServiceLoader.load(DockerClientProviderStrategy.class)},
     * which resolves against the context classloader by default. That classloader sees a
     * different (already-loaded, incompatible) copy of the testcontainers service classes
     * than the one this extension class was loaded with, so the {@code ServiceLoader} finds
     * two versions of the same service type and rejects it as "not a subtype". Forcing the
     * context classloader to this class's own loader for the duration of the call guarantees
     * {@code ServiceLoader} resolves against the same, single copy of testcontainers that this
     * extension itself uses.
     */
    private static boolean isDockerAvailable() {
        Thread currentThread = Thread.currentThread();
        ClassLoader previous = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(DockerAvailableCondition.class.getClassLoader());
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } finally {
            currentThread.setContextClassLoader(previous);
        }
    }
}
