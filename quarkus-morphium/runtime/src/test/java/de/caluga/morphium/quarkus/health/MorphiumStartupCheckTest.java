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
package de.caluga.morphium.quarkus.health;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MorphiumStartupCheck#isEverConnected}, the SRV-discovery-tolerant
 * startup check logic.
 *
 * <p>These tests call the production method directly (it is package-private specifically for
 * this reason — see its Javadoc) rather than a duplicated copy of its formula, so a future edit
 * that breaks the logic (e.g. flipping {@code ||} to {@code &&}) is guaranteed to be caught here.
 */
@DisplayName("MorphiumStartupCheck — SRV discovery tolerance")
class MorphiumStartupCheckTest {

    @Test
    @DisplayName("DOWN when no connections opened and driver not connected (SRV discovery in progress)")
    void downDuringSrvDiscovery() {
        assertThat(MorphiumStartupCheck.isEverConnected(0.0, false)).isFalse();
    }

    @Test
    @DisplayName("UP when connections opened but driver reports not connected (hosts map empty)")
    void upWhenConnectionsOpenedButHostsMapEmpty() {
        assertThat(MorphiumStartupCheck.isEverConnected(5.0, false)).isTrue();
    }

    @Test
    @DisplayName("UP when driver reports connected (normal operation)")
    void upWhenDriverConnected() {
        assertThat(MorphiumStartupCheck.isEverConnected(10.0, true)).isTrue();
    }

    @Test
    @DisplayName("UP when driver connected but no connections opened (InMemoryDriver)")
    void upWhenDriverConnectedNoConnectionsOpened() {
        assertThat(MorphiumStartupCheck.isEverConnected(0.0, true)).isTrue();
    }

    @Test
    @DisplayName("DOWN when neither signal indicates a connection")
    void downWhenNeitherSignalConnected() {
        assertThat(MorphiumStartupCheck.isEverConnected(0.0, false)).isFalse();
    }
}
