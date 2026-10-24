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

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.quarkus.messaging.MorphiumMessageListener;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** MOR-033: a CDI bean that listens to the topic "mor033"; nobody injects it. */
@ApplicationScoped
@MorphiumMessageListener(topic = "mor033")
public class EchoMessageListener implements MessageListener<Msg> {

    static final List<String> RECEIVED = new CopyOnWriteArrayList<>();

    @Override
    public Msg onMessage(MorphiumMessaging messaging, Msg m) {
        RECEIVED.add(m.getMsg());
        return null;
    }
}
