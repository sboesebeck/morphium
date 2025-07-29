package de.caluga.morphium.messaging;

import de.caluga.morphium.Morphium;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface Messaging {
    List<StdMessaging> getAlternativeMessagings();

    String getMessagingImplementation();
    void start();

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

    List<String> getGlobalListeners();

    Map<String, Long> getThreadPoolStats();

    long getPendingMessagesCount();

    void removeMessage(Msg m);

    void run();

    int getAsyncMessagesPending();

    void pauseProcessingOfMessagesNamed(String name);

    List<String> getPausedMessageNames();

    @SuppressWarnings("CommentedOutCode")
    Long unpauseProcessingOfMessagesNamed(String name);

    MsgLock getLock(Msg m);

    String getLockCollectionName();

    boolean lockMessage(Msg m, String lockId);

    boolean lockMessage(Msg m, String lockId, Date delAt);

    String getCollectionName();

    void addListenerForMessageNamed(String n, MessageListener l);

    void removeListenerForMessageNamed(String n, MessageListener l);

    String getSenderId();

    StdMessaging setSenderId(String id);

    int getPause();

    StdMessaging setPause(int pause);

    boolean isRunning();

    void terminate();

    void addMessageListener(MessageListener l);

    void removeMessageListener(MessageListener l);

    void queueMessage(Msg m);

    void sendMessage(Msg m);

    long getNumberOfMessages();

    void sendMessageToSelf(Msg m);

    void queueMessagetoSelf(Msg m);

    boolean isAutoAnswer();

    StdMessaging setAutoAnswer(boolean autoAnswer);

    <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs);

    <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs, StdMessaging.AsyncMessageCallback cb);

    <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs, boolean throwExceptionOnTimeout);

    <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout);

    <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout, boolean throwExceptionOnTimeout);

    boolean isProcessMultiple();

    @Deprecated
    StdMessaging setProcessMultiple(boolean processMultiple);

    String getQueueName();

    StdMessaging setQueueName(String queueName);

    boolean isMultithreadded();

    StdMessaging setMultithreadded(boolean multithreadded);

    int getWindowSize();

    StdMessaging setWindowSize(int windowSize);

    boolean isUseChangeStream();

    int getRunningTasks();

    Morphium getMorphium();

    StdMessaging setPolling(boolean doPolling);

    StdMessaging setUseChangeStream(boolean useChangeStream);
}
