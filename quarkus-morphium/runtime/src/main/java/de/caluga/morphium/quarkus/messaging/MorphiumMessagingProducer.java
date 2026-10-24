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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.quarkus.MorphiumRuntimeConfig;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableBean;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CDI integration of Morphium's message queue (MOR-033): an application scoped {@link MorphiumMessaging} on the default {@link Morphium} instance, created on first use,
 * configured by {@code quarkus.morphium.messaging.*}, started, given the beans that carry {@link MorphiumMessageListener}, and terminated when the application stops.
 * Because a listener has to receive messages although nobody injects the messaging, it is created at startup as soon as there is such a bean.
 */
@ApplicationScoped
public class MorphiumMessagingProducer {

    private static final Logger log = LoggerFactory.getLogger(MorphiumMessagingProducer.class);

    @Inject
    Morphium morphium;

    @Inject
    MorphiumRuntimeConfig config;

    @Produces
    @ApplicationScoped
    public MorphiumMessaging messaging() {
        try {
            MessagingSettings settings = morphium.getConfig().createCopy().messagingSettings();
            MorphiumMessagingConfig c = config.messaging();
            c.queueName().ifPresent(settings::setMessageQueueName);
            c.windowSize().ifPresent(settings::setMessagingWindowSize);
            c.pollPause().ifPresent(settings::setMessagingPollPause);
            c.multithreaded().ifPresent(settings::setMessagingMultithreadded);
            c.useChangeStream().ifPresent(settings::setUseChangeStream);
            c.senderId().ifPresent(settings::setSenderId);
            c.autoAnswer().ifPresent(settings::setAutoAnswer);

            MorphiumMessaging messaging = morphium.createMessaging(settings);
            messaging.start();

            for (InjectableBean<?> bean : listenerBeans()) {
                MorphiumMessageListener annotation = bean.getBeanClass().getAnnotation(MorphiumMessageListener.class);
                Object listener = Arc.container().instance((InjectableBean<Object>) bean).get();

                if (!(listener instanceof MessageListener)) {
                    throw new IllegalStateException(bean.getBeanClass().getName() + " is annotated with @MorphiumMessageListener but does not implement MessageListener");
                }

                for (String topic : annotation.topic()) {
                    messaging.addListenerForTopic(topic, (MessageListener) listener);
                    log.info("Registered {} as the listener of the topic '{}'", bean.getBeanClass().getSimpleName(), topic);
                }
            }

            return messaging;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Could not create the Morphium messaging: " + e.getMessage(), e);
        }
    }

    void close(@Disposes MorphiumMessaging messaging) {
        try {
            messaging.terminate();
        } catch (Exception e) {
            log.warn("Error while terminating the Morphium messaging", e);
        }
    }

    /** the beans that carry the annotation: every bean is looked at, an annotation on the class is all that identifies a listener */
    @SuppressWarnings("unchecked")
    private static List<InjectableBean<?>> listenerBeans() {
        List<InjectableBean<?>> found = new ArrayList<>();

        for (Bean<?> bean : Arc.container().beanManager().getBeans(Object.class, Any.Literal.INSTANCE)) {
            if (bean instanceof InjectableBean<?> injectable && bean.getBeanClass().isAnnotationPresent(MorphiumMessageListener.class)) {
                found.add(injectable);
            }
        }

        return found;
    }

    /** creates the messaging at startup if a bean wants to listen, so that its messages are processed without anybody injecting the messaging */
    void onStart(@Observes StartupEvent event) {
        if (!listenerBeans().isEmpty()) {
            Arc.container().instance(MorphiumMessaging.class).get().isRunning();
        }
    }
}
