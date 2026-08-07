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
package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.quarkus.MorphiumDevUIJsonRpcService;
import io.quarkus.deployment.IsDevelopment;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.devui.spi.JsonRPCProvidersBuildItem;
import io.quarkus.devui.spi.page.CardPageBuildItem;
import io.quarkus.devui.spi.page.Page;

/**
 * Registers the Morphium extension in the Quarkus Dev UI.
 *
 * <p>Uses a runtime {@link MorphiumDevUIJsonRpcService} to display the actual
 * MongoDB connection state (including auto-detected replica set mode) in the
 * Dev UI at {@code /q/dev-ui/}.
 */
public class MorphiumDevUIProcessor {

    @BuildStep(onlyIf = IsDevelopment.class)
    JsonRPCProvidersBuildItem registerJsonRpcService() {
        return new JsonRPCProvidersBuildItem(MorphiumDevUIJsonRpcService.class);
    }

    @BuildStep(onlyIf = IsDevelopment.class)
    void createCard(BuildProducer<CardPageBuildItem> cardProducer) {

        CardPageBuildItem card = new CardPageBuildItem();

        // --- Library version labels (shown at card footer, like Kafka/ArC) ---
        card.addLibraryVersion("de.caluga", "morphium",
                "Morphium", "https://github.com/sboesebeck/morphium");
        card.addLibraryVersion("de.caluga", "quarkus-morphium",
                "Quarkus Morphium Extension", "https://github.com/sboesebeck/morphium");
        card.addLibraryVersion("jakarta.data", "jakarta.data-api",
                "Jakarta Data", "https://jakarta.ee/specifications/data/");

        // --- MongoDB Connection page (runtime data via JsonRPC) ---
        card.addPage(Page.webComponentPageBuilder()
                .title("MongoDB Connection")
                .icon("font-awesome-solid:database")
                .componentLink("qwc-morphium-connection.js"));

        cardProducer.produce(card);
    }
}
