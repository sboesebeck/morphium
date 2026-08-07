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

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings;
import io.quarkus.runtime.ImageMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link MorphiumProducer#applyIndexCheckMode} -- specifically the
 * {@code CREATE_ON_WRITE_NEW_COL} branch, which needs both the index and the capped check
 * disabled when running as a native image.
 *
 * <p>Background: {@code setAutoIndexAndCappedCreationOnWrite(true)} sets BOTH checks to
 * {@code CREATE_ON_WRITE_NEW_COL}, and {@code Morphium.initializeAndConnect()} calls
 * {@code checkCapped()} unconditionally (unlike {@code checkIndices()}, which is gated to the
 * two startup modes). A live ClassGraph scan from there cannot work in a native image.
 */
@DisplayName("MorphiumProducer.applyIndexCheckMode")
class MorphiumProducerIndexCheckModeTest {

    @Test
    @DisplayName("CREATE_ON_WRITE_NEW_COL in a native image disables BOTH the index and the capped check")
    void createOnWriteNewCol_native_disablesIndexAndCappedCheck() {
        MorphiumConfig cfg = new MorphiumConfig();

        MorphiumProducer.applyIndexCheckMode(cfg,
                MorphiumRuntimeConfig.IndexCheckMode.CREATE_ON_WRITE_NEW_COL,
                ImageMode.NATIVE_RUN);

        assertThat(cfg.collectionCheckSettings().getIndexCheck())
                .as("index check must be off: checkIndices() would scan the classpath")
                .isEqualTo(CollectionCheckSettings.IndexCheck.NO_CHECK);
        assertThat(cfg.collectionCheckSettings().getCappedCheck())
                .as("capped check must be off too -- checkCapped() runs UNCONDITIONALLY in "
                        + "Morphium.initializeAndConnect(), so disabling only the index check "
                        + "would leave the ClassGraph scan reachable")
                .isEqualTo(CollectionCheckSettings.CappedCheck.NO_CHECK);
    }

    @Test
    @DisplayName("CREATE_ON_WRITE_NEW_COL on the JVM keeps create-on-write active for both checks")
    void createOnWriteNewCol_jvm_keepsCreateOnWriteBehaviour() {
        MorphiumConfig cfg = new MorphiumConfig();

        MorphiumProducer.applyIndexCheckMode(cfg,
                MorphiumRuntimeConfig.IndexCheckMode.CREATE_ON_WRITE_NEW_COL,
                ImageMode.JVM);

        // On the JVM the scan is merely a startup cost, not fatal, so the mode must keep doing
        // what the user asked for: create indexes/capped collections on first write.
        assertThat(cfg.collectionCheckSettings().getIndexCheck())
                .isEqualTo(CollectionCheckSettings.IndexCheck.CREATE_ON_WRITE_NEW_COL);
        assertThat(cfg.collectionCheckSettings().getCappedCheck())
                .isEqualTo(CollectionCheckSettings.CappedCheck.CREATE_ON_WRITE_NEW_COL);
    }

    @Test
    @DisplayName("CREATE_ON_STARTUP defers to Producer.ensureIndices() by disabling the internal check")
    void createOnStartup_disablesInternalIndexCheck() {
        MorphiumConfig cfg = new MorphiumConfig();

        MorphiumProducer.applyIndexCheckMode(cfg,
                MorphiumRuntimeConfig.IndexCheckMode.CREATE_ON_STARTUP, ImageMode.JVM);

        assertThat(cfg.collectionCheckSettings().getIndexCheck())
                .isEqualTo(CollectionCheckSettings.IndexCheck.NO_CHECK);
    }

    @Test
    @DisplayName("NO_CHECK disables the index check")
    void noCheck_disablesIndexCheck() {
        MorphiumConfig cfg = new MorphiumConfig();

        MorphiumProducer.applyIndexCheckMode(cfg,
                MorphiumRuntimeConfig.IndexCheckMode.NO_CHECK, ImageMode.JVM);

        assertThat(cfg.collectionCheckSettings().getIndexCheck())
                .isEqualTo(CollectionCheckSettings.IndexCheck.NO_CHECK);
    }

    @Test
    @DisplayName("WARN_ON_STARTUP is passed through unchanged")
    void warnOnStartup_isPassedThrough() {
        MorphiumConfig cfg = new MorphiumConfig();

        MorphiumProducer.applyIndexCheckMode(cfg,
                MorphiumRuntimeConfig.IndexCheckMode.WARN_ON_STARTUP, ImageMode.JVM);

        assertThat(cfg.collectionCheckSettings().getIndexCheck())
                .isEqualTo(CollectionCheckSettings.IndexCheck.WARN_ON_STARTUP);
    }
}
