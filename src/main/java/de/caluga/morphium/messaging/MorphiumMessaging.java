package de.caluga.morphium.messaging;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.config.MessagingSettings;

public interface MorphiumMessaging extends Closeable {
    List<MorphiumMessaging> getAlternativeMessagings();

    void start();

    void init(Morphium m) ;
    void init(Morphium m, MessagingSettings overrides);

    void enableStatusInfoListener();

    void disableStatusInfoListener();

    String getStatusInfoListenerName();

    void setStatusInfoListenerName(String statusInfoListenerName);

    int getProcessingCount();

    int getInProgressCount();

    int waitingForAnswersCount();

    int waitingForAnswersTotalCount();

    boolean isStatusInfoListenerEnabled();

    void setStatusInfoListenerEnabled(boolean statusInfoListenerEnabled);

    Map<String, List<String>> getListenerNames();


    Map<String, Long> getThreadPoolStats();

    long getPendingMessagesCount();

    void removeMessage(Msg m);

    int getAsyncMessagesPending();

    void pauseTopicProcessing(String topic);

    List<String> getPausedTopics();

    @SuppressWarnings("CommentedOutCode")
    Long unpauseTopicProcessing(String topic);

    String getLockCollectionName();
    <T extends Msg> String getLockCollectionName(T topic);
    String getLockCollectionName(String topic);

    boolean lockMessage(Msg m, String lockId, Date delAt);
    String getCollectionName();
    String getCollectionName(String topic);
    <T extends Msg> String getCollectionName(T msg);

    String getDMCollectionName(String sender);

    void addListenerForTopic(String n, MessageListener l);

    void removeListenerForTopic(String n, MessageListener l);

    String getSenderId();

    MorphiumMessaging setSenderId(String id);

    int getPause();

    MorphiumMessaging setPause(int pause);

    boolean isRunning();

    /**
     * Returns true if messaging is fully initialized and ready to process messages.
     * This includes having change stream subscriptions active (if using change streams).
     */
    boolean isReady();

    /**
     * Waits for messaging to be fully initialized and ready to process messages.
     * This is useful in tests to ensure change stream subscriptions are active
     * before sending messages.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if messaging became ready before timeout, false if timed out
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    boolean waitForReady(long timeout, TimeUnit unit) throws InterruptedException;

    void terminate();
    void close();


    void queueMessage(Msg m);

    void sendMessage(Msg m);

    long getNumberOfMessages();

    void sendMessageToSelf(Msg m);

    void queueMessagetoSelf(Msg m);

    boolean isAutoAnswer();

    MorphiumMessaging setAutoAnswer(boolean autoAnswer);

    <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs);

    <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs, SingleCollectionMessaging.AsyncMessageCallback cb);

    <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs, boolean throwExceptionOnTimeout);

    <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout);

    <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout, boolean throwExceptionOnTimeout);

    boolean isProcessMultiple();

    @Deprecated
    MorphiumMessaging setProcessMultiple(boolean processMultiple);

    String getQueueName();

    MorphiumMessaging setQueueName(String queueName);

    boolean isMultithreadded();

    MorphiumMessaging setMultithreadded(boolean multithreadded);

    int getWindowSize();

    MorphiumMessaging setWindowSize(int windowSize);

    boolean isUseChangeStream();

    int getRunningTasks();

    Morphium getMorphium();

    MorphiumMessaging setPolling(boolean doPolling);

    MorphiumMessaging setUseChangeStream(boolean useChangeStream);

}
