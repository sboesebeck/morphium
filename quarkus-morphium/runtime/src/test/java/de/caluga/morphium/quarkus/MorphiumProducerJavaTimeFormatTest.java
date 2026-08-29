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
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.objectmapping.LocalDateTimeMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link MorphiumProducer#applyJavaTimeFormat} and the reason it exists.
 *
 * <p>The extension used to reach exactly one of the four {@code java.time} types the object mapper maps
 * specially: {@code morphium()} replaced the registered {@code LocalDateTimeMapper} with one built from
 * {@code quarkus.morphium.local-date-time.use-bson-date}, and {@code Instant}, {@code LocalDate} and
 * {@code LocalTime} were left on the legacy format with no property able to change them. Applications
 * whose timestamps are {@code Instant} -- the common choice for a stored point in time -- therefore had
 * no way to get native BSON dates, and with them native range queries, sorts and TTL indexes.
 *
 * <p>Asserted here at the mapper level rather than through a driver on purpose: what the flag changes is
 * the marshalled shape, and {@code InMemoryDriver} normalises nothing on its write path (#336), so a
 * round-trip assertion against it would pass regardless of the flag and prove nothing.
 */
@DisplayName("MorphiumProducer.applyJavaTimeFormat")
class MorphiumProducerJavaTimeFormatTest {

    @Test
    @DisplayName("the property reaches ALL FOUR java.time types, not just LocalDateTime")
    void flagCoversAllFourJavaTimeTypes() {
        var mapper = mapperWith(Optional.of(true));

        assertThat(mapper.marshallIfCustomMapped(Instant.parse("2026-08-29T10:15:30Z")))
                .as("Instant is the type the deprecated per-type property could never reach")
                .isInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(LocalDate.of(2026, 8, 29))).isInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(LocalTime.of(10, 15, 30))).isInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(LocalDateTime.of(2026, 8, 29, 10, 15, 30))).isInstanceOf(Date.class);
    }

    @Test
    @DisplayName("set to false, all four keep Morphium's legacy per-type formats")
    void flagOffKeepsLegacyFormats() {
        var mapper = mapperWith(Optional.of(false));

        assertThat(mapper.marshallIfCustomMapped(Instant.parse("2026-08-29T10:15:30Z")))
                .as("legacy Instant form is a sub-document, not a BSON date")
                .isNotInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(LocalDate.of(2026, 8, 29))).isNotInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(LocalTime.of(10, 15, 30))).isNotInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(LocalDateTime.of(2026, 8, 29, 10, 15, 30))).isNotInstanceOf(Date.class);
    }

    @Test
    @DisplayName("absent property leaves the config untouched, so existing data keeps its format")
    void absentPropertyChangesNothing() {
        MorphiumConfig cfg = new MorphiumConfig();
        boolean before = cfg.objectMappingSettings().isUseBsonDateForJavaTime();

        MorphiumProducer.applyJavaTimeFormat(cfg, Optional.empty());

        assertThat(cfg.objectMappingSettings().isUseBsonDateForJavaTime())
                .as("an absent property must not silently rewrite the on-disk format on upgrade -- the "
                        + "core flag is all-or-nothing across the four types")
                .isEqualTo(before);
    }

    @Test
    @DisplayName("the deprecated per-type override PINS LocalDateTime against the flag, which is why "
            + "morphium() skips it once the new property is set")
    void deprecatedOverrideIgnoresTheFlag() {
        var mapper = mapperWith(Optional.of(true));

        // Exactly what morphium() used to do unconditionally, reproduced here: the replacement mapper
        // holds a fixed boolean and stops consulting ObjectMappingSettings.
        mapper.registerCustomMapperFor(LocalDateTime.class, new LocalDateTimeMapper(false));

        assertThat(mapper.marshallIfCustomMapped(LocalDateTime.of(2026, 8, 29, 10, 15, 30)))
                .as("pinned to legacy despite the flag being on")
                .isNotInstanceOf(Date.class);
        assertThat(mapper.marshallIfCustomMapped(Instant.parse("2026-08-29T10:15:30Z")))
                .as("while Instant still follows the flag -- exactly the per-type split the new "
                        + "property removes, and the reason both paths must not run together")
                .isInstanceOf(Date.class);
    }

    @Test
    @DisplayName("the deprecated override is applied ONLY while the new property is absent")
    void deprecatedOverrideIsSkippedOnceTheNewPropertyIsSet() {
        assertThat(MorphiumProducer.registersDeprecatedLocalDateTimeOverride(Optional.empty()))
                .as("no opt-in: the extension must behave exactly as before")
                .isTrue();
        assertThat(MorphiumProducer.registersDeprecatedLocalDateTimeOverride(Optional.of(true)))
                .as("opted in: the override would pin LocalDateTime against the flag")
                .isFalse();
        assertThat(MorphiumProducer.registersDeprecatedLocalDateTimeOverride(Optional.of(false)))
                .as("explicit false is an opt-in too -- the flag governs all four types, including "
                        + "LocalDateTime, so the per-type override must stay out of the way")
                .isFalse();
    }

    /** An object mapper wired to a config carrying the given property value, as buildMorphium() does. */
    private ObjectMapperImpl mapperWith(Optional<Boolean> useBsonDateForJavaTime) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("test");
        // InMemDriver, because setConfig() connects (Morphium:241 -> initializeAndConnect) and would
        // otherwise fail with "no server address specified". The driver is irrelevant to what is asserted
        // here: the flag changes what the MAPPER produces, and these assertions read the mapper's output
        // directly instead of storing and reading back -- which is also the only sound way to check it,
        // since InMemoryDriver normalises nothing on its write path (#336).
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        MorphiumProducer.applyJavaTimeFormat(cfg, useBsonDateForJavaTime);

        var m = new Morphium();
        m.setConfig(cfg);

        var mapper = new ObjectMapperImpl();
        mapper.setMorphium(m);
        return mapper;
    }
}
