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

import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-033: the injected {@link MorphiumMessaging} sends a message and a CDI bean with {@code @MorphiumMessageListener} receives it, without anybody having started the
 * messaging. The subclasses run it on the in-memory driver and on a real MongoDB.
 */
@DisplayName("Morphium messaging as a CDI bean")
abstract class AbstractMorphiumMessagingCdiTest {

    @Inject
    MorphiumMessaging messaging;

    @Test
    @DisplayName("a message to the own sender reaches the annotated listener bean")
    void injectedMessagingDeliversToAnnotatedListener() throws InterruptedException {
        long until = System.currentTimeMillis() + 20_000;

        while (!messaging.isRunning() && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }

        assertThat(messaging.isRunning()).isTrue();

        Msg m = new Msg("mor033", "hello from the test", "value");
        messaging.sendMessageToSelf(m);

        until = System.currentTimeMillis() + 20_000;

        while (!EchoMessageListener.RECEIVED.contains("hello from the test") && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }

        assertThat(EchoMessageListener.RECEIVED).contains("hello from the test");
        assertThat(messaging.getListenerNames()).containsKey("mor033");
    }
}
