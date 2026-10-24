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
import de.caluga.morphium.quarkus.testing.ResetMorphiumExtension;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** MOR-093: quarkus.morphium.enum-type=ORDINAL is the setting for the enums without @Enumerated; the annotation wins. */
@QuarkusTest
@TestProfile(OrdinalEnumTestProfile.class)
@ExtendWith(ResetMorphiumExtension.class)
class MorphiumEnumTypeTest {

    @Inject
    Morphium morphium;

    @Test
    @DisplayName("the setting makes the plain field an ordinal, the annotation keeps the name, queries and sorts follow")
    void settingAndAnnotation() {
        RankedEntity high = new RankedEntity();
        high.id = "h";
        high.plain = RankedEntity.Rank.HIGH;
        high.named = RankedEntity.Rank.HIGH;
        RankedEntity low = new RankedEntity();
        low.id = "l";
        low.plain = RankedEntity.Rank.LOW;
        low.named = RankedEntity.Rank.LOW;
        morphium.store(high);
        morphium.store(low);

        Map<String, Object> doc = morphium.getMapper().serialize(high);
        assertThat(doc.get("plain")).isEqualTo(2);
        assertThat(doc.get("named")).isEqualTo("HIGH");

        assertThat(morphium.createQueryFor(RankedEntity.class).f("plain").gt(RankedEntity.Rank.LOW).asList()).extracting(r -> r.id).containsExactly("h");
        assertThat(morphium.createQueryFor(RankedEntity.class).sort(Map.of("plain", 1)).asList()).extracting(r -> r.id).containsExactly("l", "h");
    }
}
