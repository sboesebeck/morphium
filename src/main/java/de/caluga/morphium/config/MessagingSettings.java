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
    public String getMessagingStatusInfoListenerName() {
        return messagingStatusInfoListenerName;
    }
    public void setMessagingStatusInfoListenerName(String messagingStatusInfoListenerName) {
        this.messagingStatusInfoListenerName = messagingStatusInfoListenerName;
    }
    public boolean isMessagingStatusInfoListenerEnabled() {
        return messagingStatusInfoListenerEnabled;
    }
    public void setMessagingStatusInfoListenerEnabled(boolean messagingStatusInfoListenerEnabled) {
        this.messagingStatusInfoListenerEnabled = messagingStatusInfoListenerEnabled;
    }
    public String getMessagingImplementation() {
        return messagingImplementation;
    }
    public void setMessagingImplementation(String messagingImplementation) {
        this.messagingImplementation = messagingImplementation;
    }
    public int getThreadPoolMessagingCoreSize() {
        return threadPoolMessagingCoreSize;
    }
    public void setThreadPoolMessagingCoreSize(int threadPoolMessagingCoreSize) {
        this.threadPoolMessagingCoreSize = threadPoolMessagingCoreSize;
    }
    public int getThreadPoolMessagingMaxSize() {
        return threadPoolMessagingMaxSize;
    }
    public void setThreadPoolMessagingMaxSize(int threadPoolMessagingMaxSize) {
        this.threadPoolMessagingMaxSize = threadPoolMessagingMaxSize;
    }
    public long getThreadPoolMessagingKeepAliveTime() {
        return threadPoolMessagingKeepAliveTime;
    }
    public void setThreadPoolMessagingKeepAliveTime(long threadPoolMessagingKeepAliveTime) {
        this.threadPoolMessagingKeepAliveTime = threadPoolMessagingKeepAliveTime;
    }
    public int getMessagingWindowSize() {
        return messagingWindowSize;
    }
    public void setMessagingWindowSize(int messagingWindowSize) {
        this.messagingWindowSize = messagingWindowSize;
    }
    public Class<? extends Messaging> getMessagingClass() {
        return messagingClass;
    }
    public void setMessagingClass(Class<? extends Messaging> messagingClass) {
        this.messagingClass = messagingClass;
    }
}
