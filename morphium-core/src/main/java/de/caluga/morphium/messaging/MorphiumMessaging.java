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

    /**
     * Register multiple topic listeners in a single batch. All change stream monitors are started
     * in parallel, which dramatically reduces startup time compared to sequential addListenerForTopic calls.
     * For MCM with N topics, startup drops from N*2s to ~2s total.
     * Default implementation falls back to sequential addListenerForTopic calls.
     */
    default void addListenersForTopics(Map<String, MessageListener> listeners) {
        listeners.forEach(this::addListenerForTopic);
    }

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

    /**
     * Sends a list of messages as one or more bulk-insert wire calls instead of one insert per
     * message — grouped by whatever target collection this implementation's routing needs (e.g.
     * one call for all broadcasts, one per distinct recipient for directed messages). Each
     * message gets sender/senderHost/TTL defaults applied exactly as {@link #sendMessage(Msg)}
     * would. A null or empty list is a no-op.
     *
     * <p>Genuine client-side batching, not {@code @WriteBuffer}: no housekeeping thread, no
     * polling, no size/timeout tuning — the caller decides the batch, one call carries it. Pays
     * off for callers that already have several messages ready at once (bulk jobs, event
     * replay, chunked/streamed responses); a single ad-hoc {@code sendMessage()} call gains
     * nothing from being wrapped in a one-element list.
     *
     * @param messages the messages to send, in the order given (bulk inserts within a
     *                 collection are unordered — see driver bulk-write semantics for partial
     *                 failure behavior)
     */
    void sendMessages(List<? extends Msg> messages);

    /**
     * Sends a list of answers to a single original message — the bulk-send equivalent of
     * calling {@link Msg#sendAnswer(MorphiumMessaging, Msg)} once per answer. Useful for
     * streaming a large or chunked response back to one requester from a single thread: each
     * answer gets {@code inAnswerTo}/recipient/a fresh {@code msgId} set exactly as
     * {@code sendAnswer()} would, then all answers go out via {@link #sendMessages(List)}.
     *
     * @param answerOf the original message being answered
     * @param answers  the answers to send, in order; a null or empty list is a no-op
     */
    default <T extends Msg> void sendAnswers(Msg answerOf, List<T> answers) {
        if (answers == null || answers.isEmpty()) {
            return;
        }

        for (Msg a : answers) {
            a.setInAnswerTo(answerOf.getMsgId());
            a.addRecipient(answerOf.getSender());

            // see Msg#sendAnswer(): only derive deleteAt when the answer carries an explicit
            // TTL, otherwise leave it null so the send path applies messagingDefaultTtl first
            if (a.getTtl() > 0) {
                a.setDeleteAt(new java.util.Date(System.currentTimeMillis() + a.getTtl()));
            }

            a.setMsgId(new de.caluga.morphium.driver.MorphiumId());
        }

        sendMessages(answers);
    }

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

    /**
     * legacy switch for processing several messages at once. This concept is obsolete:
     * how many messages are processed in parallel is controlled by the window size, use
     * {@link #setWindowSize(int)} instead.
     *
     * @deprecated use {@link #setWindowSize(int)} instead; will be removed in 7.0
     */
    @Deprecated(since = "6.3", forRemoval = true)
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
