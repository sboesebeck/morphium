package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.StdMessaging;

@Embedded
public class MessagingSettings {
    private String messagingStatusInfoListenerName = null;
    private boolean messagingStatusInfoListenerEnabled = true;
    private String messagingImplementation = StdMessaging.messagingImplementation;
    private int threadPoolMessagingCoreSize = 0;
    private int threadPoolMessagingMaxSize = 100;
    private long threadPoolMessagingKeepAliveTime = 2000;
    private int messagingWindowSize = 100;
    @Transient
    private Class<? extends Messaging> messagingClass = StdMessaging.class;
}
