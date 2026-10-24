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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import io.quarkus.runtime.ShutdownEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The named Morphium instances (MOR-032): built on first use from {@code quarkus.morphium.named."name".*} by the same code as the default instance, closed on shutdown.
 * The CDI beans that carry {@link MorphiumInstance} are synthetic beans that ask this registry.
 */
@ApplicationScoped
public class NamedMorphiumRegistry {

    private static final Logger log = LoggerFactory.getLogger(NamedMorphiumRegistry.class);

    @Inject
    MorphiumRuntimeConfig config;

    @Inject
    MorphiumProducer producer;

    private final Map<String, Morphium> instances = new ConcurrentHashMap<>();

    /** the names that are configured, whether the instance was built yet or not */
    public List<String> names() {
        return new ArrayList<>(config.named().keySet());
    }

    /** the instance of the name, built on the first call */
    public Morphium get(String name) {
        return instances.computeIfAbsent(name, n -> {
            NamedMorphiumConfig named = config.named().get(n);

            if (named == null) {
                throw new IllegalStateException("No Morphium instance is configured as quarkus.morphium.named.\"" + n + "\".* (configured: " + config.named().keySet() + ")");
            }

            log.info("Creating the named Morphium instance '{}'", n);
            return producer.buildMorphium(named, false);
        });
    }

    void onStop(@Observes ShutdownEvent event) {
        for (Map.Entry<String, Morphium> e : instances.entrySet()) {
            try {
                e.getValue().close();
            } catch (Exception ex) {
                log.warn("Error while closing the named Morphium instance '{}'", e.getKey(), ex);
            }
        }

        instances.clear();
    }
}
