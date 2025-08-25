package de.caluga.morphium.messaging;

import java.util.Date;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.config.MessagingSettings;

public interface MorphiumMessaging {
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

    void pauseProcessingOfMessagesNamed(String name);

    List<String> getPausedMessageNames();

    @SuppressWarnings("CommentedOutCode")
    Long unpauseProcessingOfMessagesNamed(String name);

    String getLockCollectionName();

    String getCollectionName();

    void addListenerForMessageNamed(String n, MessageListener l);

    void removeListenerForMessageNamed(String n, MessageListener l);

    String getSenderId();

    MorphiumMessaging setSenderId(String id);

    int getPause();

    MorphiumMessaging setPause(int pause);

    boolean isRunning();

    void terminate();


    void queueMessage(Msg m);

    void sendMessage(Msg m);

    long getNumberOfMessages();

    void sendMessageToSelf(Msg m);

    void queueMessagetoSelf(Msg m);

    boolean isAutoAnswer();

    MorphiumMessaging setAutoAnswer(boolean autoAnswer);

    <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs);

    <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs, StdMessaging.AsyncMessageCallback cb);

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
