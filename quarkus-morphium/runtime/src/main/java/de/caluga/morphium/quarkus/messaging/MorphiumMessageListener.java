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
package de.caluga.morphium.quarkus.messaging;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Registers a CDI bean that implements {@link de.caluga.morphium.messaging.MessageListener} as the listener of a topic of the injected
 * {@link de.caluga.morphium.messaging.MorphiumMessaging} (MOR-033):
 *
 * <pre>{@code
 * @ApplicationScoped
 * @MorphiumMessageListener(topic = "orders")
 * public class OrderListener implements MessageListener<Msg> {
 *     public Msg onMessage(MorphiumMessaging messaging, Msg m) { ...; return null; }
 * }
 * }</pre>
 *
 * The messaging is started with the application as soon as there is such a bean, and the listeners are registered before the first message is processed. The bean keeps its
 * own scope; it is not removed as unused.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MorphiumMessageListener {

    /** the topic the bean listens to; several beans may share a topic, and a bean may be annotated once per topic by listing them */
    String[] topic();
}
