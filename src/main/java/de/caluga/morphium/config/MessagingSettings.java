package de.caluga.morphium.config;

import java.util.UUID;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;

@Embedded
public class MessagingSettings extends Settings {
    private String messagingStatusInfoListenerName = null;
    private boolean messagingStatusInfoListenerEnabled = true;
    private boolean useChangeStream = true;
    private boolean messagingMultithreadded = true;
    private String messagingImplementation = "StandardMessaging";
    private String messageQueueName = "msg";
    private int threadPoolMessagingCoreSize = 0;
    private int threadPoolMessagingMaxSize = 100;
    private long threadPoolMessagingKeepAliveTime = 2000;
    private int messagingWindowSize = 100;
    private int messagingPollPause = 250;
    private String senderId = null;
    private boolean autoAnswer = false;
    private boolean processMultiple = true;

    @Transient
    // Default class remains StdMessaging for backward compatibility; new alias SingleCollectionMessaging is also available
    private Class <? extends MorphiumMessaging > messagingClass = SingleCollectionMessaging.class;



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
        if (messagingImplementation == null) {
            messagingImplementation = "SingleCollectionMessaging";
        }
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
    public boolean isUseChangeStream() {
        return useChangeStream;
    }
    public void setUseChangeStream(boolean useChangeStrean) {
        this.useChangeStream = useChangeStrean;
    }
    public String getMessageQueueName() {
        return messageQueueName;
    }
    public void setMessageQueueName(String queueName) {
        this.messageQueueName = queueName;
    }
    public int getMessagingPollPause() {
        return messagingPollPause;
    }
    public void setMessagingPollPause(int messagingPollPause) {
        this.messagingPollPause = messagingPollPause;
    }
    public boolean isMessagingMultithreadded() {
        return messagingMultithreadded;
    }
    public void setMessagingMultithreadded(boolean messagingMultithreadded) {
        this.messagingMultithreadded = messagingMultithreadded;
    }
    public String getSenderId() {
        return senderId;
    }
    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }
    public boolean isAutoAnswer() {
        return autoAnswer;
    }
    public void setAutoAnswer(boolean autoAnswer) {
        this.autoAnswer = autoAnswer;
    }
    public boolean isProcessMultiple() {
        return processMultiple;
    }
    public void setProcessMultiple(boolean processMultiple) {
        this.processMultiple = processMultiple;
    }
}
