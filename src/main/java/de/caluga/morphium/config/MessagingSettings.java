package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.StdMessaging;

@Embedded
public class MessagingSettings {
    private String messagingStatusInfoListenerName = null;
    private boolean messagingStatusInfoListenerEnabled = true;
    private String messagingImplementation = "StandardMessaging";
    private int threadPoolMessagingCoreSize = 0;
    private int threadPoolMessagingMaxSize = 100;
    private long threadPoolMessagingKeepAliveTime = 2000;
    private int messagingWindowSize = 100;


    @Transient
    private Class <? extends MorphiumMessaging > messagingClass = StdMessaging.class;
    public String getMessagingStatusInfoListenerName() {
        return messagingStatusInfoListenerName;
    }
    public MessagingSettings setMessagingStatusInfoListenerName(String messagingStatusInfoListenerName) {
        this.messagingStatusInfoListenerName = messagingStatusInfoListenerName;
        return this;
    }
    public boolean isMessagingStatusInfoListenerEnabled() {
        return messagingStatusInfoListenerEnabled;
    }
    public MessagingSettings setMessagingStatusInfoListenerEnabled(boolean messagingStatusInfoListenerEnabled) {
        this.messagingStatusInfoListenerEnabled = messagingStatusInfoListenerEnabled;
        return this;
    }
    public String getMessagingImplementation() {
        return messagingImplementation;
    }
    public MessagingSettings setMessagingImplementation(String messagingImplementation) {
        this.messagingImplementation = messagingImplementation;
        return this;
    }
    public int getThreadPoolMessagingCoreSize() {
        return threadPoolMessagingCoreSize;
    }
    public MessagingSettings setThreadPoolMessagingCoreSize(int threadPoolMessagingCoreSize) {
        this.threadPoolMessagingCoreSize = threadPoolMessagingCoreSize;
        return this;
    }
    public int getThreadPoolMessagingMaxSize() {
        return threadPoolMessagingMaxSize;
    }
    public MessagingSettings setThreadPoolMessagingMaxSize(int threadPoolMessagingMaxSize) {
        this.threadPoolMessagingMaxSize = threadPoolMessagingMaxSize;
        return this;
    }
    public long getThreadPoolMessagingKeepAliveTime() {
        return threadPoolMessagingKeepAliveTime;
    }
    public MessagingSettings setThreadPoolMessagingKeepAliveTime(long threadPoolMessagingKeepAliveTime) {
        this.threadPoolMessagingKeepAliveTime = threadPoolMessagingKeepAliveTime;
        return this;
    }
    public int getMessagingWindowSize() {
        return messagingWindowSize;
    }
    public MessagingSettings setMessagingWindowSize(int messagingWindowSize) {
        this.messagingWindowSize = messagingWindowSize;
        return this;
    }
    public Class <? extends MorphiumMessaging > getMessagingClass() {
        return messagingClass;
    }
    public MessagingSettings setMessagingClass(Class <? extends MorphiumMessaging > messagingClass) {
        this.messagingClass = messagingClass;
        return this;
    }
}
