package de.caluga.morphium.messaging;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Messaging;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.messaging.StdMessaging.AsyncMessageCallback;



/**
 * MessageQueueing implementation with additional features:
 * - using a collection for each message name - reducing load on clients with minimal overhead on MongoDB-Side
 * - adding capability for streaming data. Usually as an answer, but could also be as "just as is"
*/
@Messaging(name = "AdvMessaging", description =
                           "Message queueing implementation, that splits messages into different collections in order to reduce overhead on client side and improve effectiveness of changestreams")
public class AdvancedSplitCollectionMessaging implements MorphiumMessaging {

    private Morphium morphium;
    private MessagingSettings effectiveSettings;

    private static Vector<AdvancedSplitCollectionMessaging> allMessagings = new Vector<>();
    @Override
    public List<MorphiumMessaging> getAlternativeMessagings() {
        return new Vector<>(allMessagings);
    }

    @Override
    public void start() {
    }

    @Override
    public void enableStatusInfoListener() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'enableStatusInfoListener'");
    }

    @Override
    public void disableStatusInfoListener() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'disableStatusInfoListener'");
    }

    @Override
    public String getStatusInfoListenerName() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStatusInfoListenerName'");
    }

    @Override
    public void setStatusInfoListenerName(String statusInfoListenerName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setStatusInfoListenerName'");
    }

    @Override
    public int getProcessingCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getProcessingCount'");
    }

    @Override
    public int getInProgressCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getInProgressCount'");
    }

    @Override
    public int waitingForAnswersCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'waitingForAnswersCount'");
    }

    @Override
    public int waitingForAnswersTotalCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'waitingForAnswersTotalCount'");
    }

    @Override
    public boolean isStatusInfoListenerEnabled() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isStatusInfoListenerEnabled'");
    }

    @Override
    public void setStatusInfoListenerEnabled(boolean statusInfoListenerEnabled) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setStatusInfoListenerEnabled'");
    }

    @Override
    public Map<String, List<String>> getListenerNames() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getListenerNames'");
    }

    @Override
    public List<String> getGlobalListeners() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGlobalListeners'");
    }

    @Override
    public Map<String, Long> getThreadPoolStats() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getThreadPoolStats'");
    }

    @Override
    public long getPendingMessagesCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getPendingMessagesCount'");
    }

    @Override
    public void removeMessage(Msg m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeMessage'");
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'run'");
    }

    @Override
    public int getAsyncMessagesPending() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAsyncMessagesPending'");
    }

    @Override
    public void pauseProcessingOfMessagesNamed(String name) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'pauseProcessingOfMessagesNamed'");
    }

    @Override
    public List<String> getPausedMessageNames() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getPausedMessageNames'");
    }

    @Override
    public Long unpauseProcessingOfMessagesNamed(String name) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'unpauseProcessingOfMessagesNamed'");
    }

    @Override
    public MsgLock getLock(Msg m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLock'");
    }

    @Override
    public String getLockCollectionName() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLockCollectionName'");
    }

    @Override
    public boolean lockMessage(Msg m, String lockId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'lockMessage'");
    }

    @Override
    public boolean lockMessage(Msg m, String lockId, Date delAt) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'lockMessage'");
    }

    @Override
    public String getCollectionName() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCollectionName'");
    }

    @Override
    public void addListenerForMessageNamed(String n, MessageListener l) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addListenerForMessageNamed'");
    }

    @Override
    public void removeListenerForMessageNamed(String n, MessageListener l) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeListenerForMessageNamed'");
    }

    @Override
    public String getSenderId() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSenderId'");
    }

    @Override
    public StdMessaging setSenderId(String id) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setSenderId'");
    }

    @Override
    public int getPause() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getPause'");
    }

    @Override
    public StdMessaging setPause(int pause) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setPause'");
    }

    @Override
    public boolean isRunning() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isRunning'");
    }

    @Override
    public void terminate() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'terminate'");
    }

    @Override
    public void addMessageListener(MessageListener l) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addMessageListener'");
    }

    @Override
    public void removeMessageListener(MessageListener l) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeMessageListener'");
    }

    @Override
    public void queueMessage(Msg m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'queueMessage'");
    }

    @Override
    public void sendMessage(Msg m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendMessage'");
    }

    @Override
    public long getNumberOfMessages() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getNumberOfMessages'");
    }

    @Override
    public void sendMessageToSelf(Msg m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendMessageToSelf'");
    }

    @Override
    public void queueMessagetoSelf(Msg m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'queueMessagetoSelf'");
    }

    @Override
    public boolean isAutoAnswer() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isAutoAnswer'");
    }

    @Override
    public StdMessaging setAutoAnswer(boolean autoAnswer) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setAutoAnswer'");
    }

    @Override
    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAndAwaitFirstAnswer'");
    }

    @Override
    public <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs, AsyncMessageCallback cb) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAndAwaitAsync'");
    }

    @Override
    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs, boolean throwExceptionOnTimeout) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAndAwaitFirstAnswer'");
    }

    @Override
    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAndAwaitAnswers'");
    }

    @Override
    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout,
            boolean throwExceptionOnTimeout) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAndAwaitAnswers'");
    }

    @Override
    public boolean isProcessMultiple() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isProcessMultiple'");
    }

    @Override
    public StdMessaging setProcessMultiple(boolean processMultiple) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setProcessMultiple'");
    }

    @Override
    public String getQueueName() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getQueueName'");
    }

    @Override
    public StdMessaging setQueueName(String queueName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setQueueName'");
    }

    @Override
    public boolean isMultithreadded() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isMultithreadded'");
    }

    @Override
    public StdMessaging setMultithreadded(boolean multithreadded) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setMultithreadded'");
    }

    @Override
    public int getWindowSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getWindowSize'");
    }

    @Override
    public StdMessaging setWindowSize(int windowSize) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setWindowSize'");
    }

    @Override
    public boolean isUseChangeStream() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isUseChangeStream'");
    }

    @Override
    public int getRunningTasks() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRunningTasks'");
    }

    @Override
    public Morphium getMorphium() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMorphium'");
    }

    @Override
    public StdMessaging setPolling(boolean doPolling) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setPolling'");
    }

    @Override
    public StdMessaging setUseChangeStream(boolean useChangeStream) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setUseChangeStream'");
    }

    @Override
    public void init(Morphium m) {
        init(m, m.getConfig().messagingSettings());
    }

    @Override
    public void init(Morphium m, MessagingSettings overrides) {
        morphium = m;
        if (overrides == m.getConfig().messagingSettings()) {
            effectiveSettings = m.getConfig().createCopy().messagingSettings();
        } else {

        }
    }

}
