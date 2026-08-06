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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@link MorphiumProducer#validateCredentialsPresence} and
 * {@link MorphiumProducer#toIntGlobalCacheValidTime}.
 *
 * <p>Previously, {@code buildMorphium()} silently connected unauthenticated when only one of
 * {@code quarkus.morphium.username}/{@code password} was set, and silently overflowed
 * {@code quarkus.morphium.cache.global-valid-time} values above ~24.8 days via a direct
 * {@code (int)} cast. Both are should-fix findings from Stephan Boesebeck's review on PR #267
 * (sboesebeck/morphium).
 */
@DisplayName("MorphiumProducer — config validation")
class MorphiumProducerConfigValidationTest {

    @Test
    @DisplayName("validateCredentialsPresence: both present is valid")
    void bothCredentialsPresent_isValid() {
        MorphiumProducer.validateCredentialsPresence(true, true);
        // no exception -- success
    }

    @Test
    @DisplayName("validateCredentialsPresence: both absent is valid")
    void bothCredentialsAbsent_isValid() {
        MorphiumProducer.validateCredentialsPresence(false, false);
        // no exception -- success
    }

    @Test
    @DisplayName("validateCredentialsPresence: username without password throws")
    void usernameWithoutPassword_throws() {
        assertThatThrownBy(() -> MorphiumProducer.validateCredentialsPresence(true, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("quarkus.morphium.password")
                .hasMessageContaining("quarkus.morphium.username");
    }

    @Test
    @DisplayName("validateCredentialsPresence: password without username throws")
    void passwordWithoutUsername_throws() {
        assertThatThrownBy(() -> MorphiumProducer.validateCredentialsPresence(false, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("quarkus.morphium.username")
                .hasMessageContaining("quarkus.morphium.password");
    }

    @Test
    @DisplayName("toIntGlobalCacheValidTime: default (60000ms) narrows without loss")
    void defaultValue_narrowsCorrectly() {
        assertThat(MorphiumProducer.toIntGlobalCacheValidTime(60000L)).isEqualTo(60000);
    }

    @Test
    @DisplayName("toIntGlobalCacheValidTime: Integer.MAX_VALUE itself is still accepted")
    void maxIntValue_isAccepted() {
        assertThat(MorphiumProducer.toIntGlobalCacheValidTime((long) Integer.MAX_VALUE))
                .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    @DisplayName("toIntGlobalCacheValidTime: a 30-day value (which would silently overflow via a raw cast) throws instead")
    void thirtyDayValue_throwsInsteadOfOverflowing() {
        long thirtyDaysMs = 30L * 24 * 60 * 60 * 1000; // 2_592_000_000 -- overflows int
        assertThatThrownBy(() -> MorphiumProducer.toIntGlobalCacheValidTime(thirtyDaysMs))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("global-valid-time")
                .hasMessageContaining(String.valueOf(thirtyDaysMs));
    }
}
