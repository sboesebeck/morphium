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

import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link MorphiumProducer#parseReadPreference}.
 *
 * <p>Previously, {@code buildMorphium()} called
 * {@code cfg.driverSettings().setDefaultReadPreferenceType(config.readPreference())}, which at the
 * time set a dead {@code defaultReadPreferenceType} string field that nothing in morphium-core
 * read. The actual read path uses {@code DriverSettings.getDefaultReadPreference()}, which defaults
 * to {@code ReadPreference.nearest()} regardless of what
 * {@code quarkus.morphium.read-preference} was configured to. Every app on a replica set
 * therefore read with NEAREST instead of the documented default {@code primary} (stale reads out
 * of the box), and no value of the setting changed that. Fixed by parsing the string into a real
 * {@link ReadPreference} and calling {@code setDefaultReadPreference(ReadPreference)} instead.
 *
 * <p>The string field is no longer dead: it is now the serializable carrier that keeps the
 * preference across a properties round trip, and setting it rebuilds the preference object. Going
 * through {@code setDefaultReadPreference(ReadPreference)} remains the right call here, because it
 * is the only one that can carry a tag set.
 */
class MorphiumProducerReadPreferenceTest {

    @Test
    @DisplayName("\"primary\" parses to ReadPreference.primary()")
    void primary() {
        assertThat(MorphiumProducer.parseReadPreference("primary").getType())
                .isEqualTo(ReadPreferenceType.PRIMARY);
    }

    @Test
    @DisplayName("\"primaryPreferred\" parses to ReadPreference.primaryPreferred()")
    void primaryPreferred() {
        assertThat(MorphiumProducer.parseReadPreference("primaryPreferred").getType())
                .isEqualTo(ReadPreferenceType.PRIMARY_PREFERRED);
    }

    @Test
    @DisplayName("\"secondary\" parses to ReadPreference.secondary()")
    void secondary() {
        assertThat(MorphiumProducer.parseReadPreference("secondary").getType())
                .isEqualTo(ReadPreferenceType.SECONDARY);
    }

    @Test
    @DisplayName("\"secondaryPreferred\" parses to ReadPreference.secondaryPreferred()")
    void secondaryPreferred() {
        assertThat(MorphiumProducer.parseReadPreference("secondaryPreferred").getType())
                .isEqualTo(ReadPreferenceType.SECONDARY_PREFERRED);
    }

    @Test
    @DisplayName("\"nearest\" parses to ReadPreference.nearest()")
    void nearest() {
        assertThat(MorphiumProducer.parseReadPreference("nearest").getType())
                .isEqualTo(ReadPreferenceType.NEAREST);
    }

    @Test
    @DisplayName("Case-insensitive matching")
    void caseInsensitive() {
        assertThat(MorphiumProducer.parseReadPreference("PRIMARY").getType())
                .isEqualTo(ReadPreferenceType.PRIMARY);
    }

    @Test
    @DisplayName("Unrecognized value falls back to primary(), matching the documented default")
    void unrecognizedValueFallsBackToPrimary() {
        assertThat(MorphiumProducer.parseReadPreference("bogus").getType())
                .isEqualTo(ReadPreferenceType.PRIMARY);
    }
}
