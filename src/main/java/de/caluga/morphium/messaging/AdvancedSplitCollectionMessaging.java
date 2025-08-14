package de.caluga.morphium.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Messaging;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.messaging.StdMessaging.AsyncMessageCallback;
import de.caluga.morphium.query.Query;



/**
 * MessageQueueing implementation with additional features:
 * - using a collection for each message name - reducing load on clients with minimal overhead on MongoDB-Side
 * - adding capability for streaming data. Usually as an answer, but could also be as "just as is"
*/
@Messaging(name = "AdvMessaging", description =
        "Message queueing implementation, that splits messages into different collections in order to reduce overhead on client side and improve effectiveness of changestreams")
public class AdvancedSplitCollectionMessaging implements MorphiumMessaging {

    private Logger log = LoggerFactory.getLogger(AdvancedSplitCollectionMessaging.class);
    private Morphium morphium;
    private MessagingSettings effectiveSettings;
    private ThreadPoolExecutor threadPool;


    private Map < String, List< Map<String, Object>>> monitorsByMsgName = new ConcurrentHashMap<>();
    private AtomicBoolean running = new AtomicBoolean(false);
    private String lockCollectionName;
    private static Vector<AdvancedSplitCollectionMessaging> allMessagings = new Vector<>();

    @Override
    public List<MorphiumMessaging> getAlternativeMessagings() {
        return new Vector<>(allMessagings);
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void enableStatusInfoListener() {
        effectiveSettings.setMessagingStatusInfoListenerEnabled(true);
    }

    @Override
    public void disableStatusInfoListener() {
        effectiveSettings.setMessagingStatusInfoListenerEnabled(false);
    }

    @Override
    public String getStatusInfoListenerName() {
        return effectiveSettings.getMessagingStatusInfoListenerName();
    }

    @Override
    public void setStatusInfoListenerName(String statusInfoListenerName) {
        effectiveSettings.setMessagingStatusInfoListenerName(statusInfoListenerName);
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
    public String getCollectionName() {
        return effectiveSettings.getMessageQueueName();
    }

    public String getCollectionName(Msg m) {
        return getCollectionName(m.getName());
    }
    public String getCollectionName(String n) {
        return (getCollectionName() + "_" + n).replaceAll(" ", "").replaceAll("-", "").replaceAll("/", "");
    }
    public MsgLock getLock(Msg m) {
        return morphium.findById(MsgLock.class, m.getMsgId(), getLockCollectionName() + "_" + m.getName());
    }

    @Override
    public String getLockCollectionName() {
        if (lockCollectionName == null) {
            lockCollectionName = getCollectionName() + "_lck";
        }

        return lockCollectionName;
    }

    public String getLockCollectionName(Msg m) {
        return getLockCollectionName(m.getName());
    }

    public String getLockCollectionName(String name) {
        //TODO: improve
        return (getLockCollectionName() + "_" + name).replaceAll(" ", "").replaceAll("-", "").replaceAll("/", "");
    }
    public boolean lockMessage(Msg m, String lockId) {
        return lockMessage(m, lockId, null);
    }

    public boolean lockMessage(Msg m, String lockId, Date delAt) {
        MsgLock lck = new MsgLock(m);
        lck.setLockId(lockId);

        if (delAt != null) {
            lck.setDeleteAt(delAt);
        }

        InsertMongoCommand cmd = null;

        try {
            cmd = new InsertMongoCommand(morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(MsgLock.class)));
            cmd.setColl(getLockCollectionName(m)).setDb(morphium.getDatabase()).setDocuments(List.of(morphium.getMapper().serialize(lck)));
            cmd.execute();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }



    private void updateProcessedBy(Msg msg) {
        if (msg == null) {
            return;
        }

        if (msg.getProcessedBy().contains(getSenderId())) {
            return;
        }

        String id = getSenderId();
        msg.getProcessedBy().add(getSenderId());
        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName(msg));
        idq.f(Msg.Fields.msgId).eq(msg.getMsgId());
        Map<String, Object> qobj = idq.toQueryObject();
        Map<String, Object> set = Doc.of("processed_by", getSenderId());
        Map<String, Object> update = Doc.of("$addToSet", set);
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
            cmd.addUpdate(qobj, update, null, false, false, null, null, null);
            Map<String, Object> ret = cmd.execute();
            cmd.releaseConnection();
            cmd = null;

            // log.debug("Updating processed by for "+id+" on message "+msg.getMsgId());
            if (ret.get("nModified") == null && ret.get("modified") == null || Integer.valueOf(0).equals(ret.get("nModified"))) {
                if (morphium.reread(msg, getCollectionName()) != null) {
                    if (!msg.getProcessedBy().contains(id)) {
                        log.warn(id + ": Could not update processed_by in msg " + msg.getMsgId());
                        log.warn(id + ": " + Utils.toJsonString(ret));
                        log.warn(id + ": msg: " + msg.toString());
                    }

                    // } else {
                    // log.debug("message deleted by someone else!!!");
                }
            }
        } catch (MorphiumDriverException e) {
            log.error("Error updating processed by - this might lead to duplicate execution!", e);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    @Override
    public void addListenerForMessageNamed(String n, MessageListener l) {
        Map<String, Object> match = new LinkedHashMap<>();
        Map<String, Object> in = new LinkedHashMap<>();
        //        in.put("$eq", "insert"); //, "delete", "update"));
        in.put("$in", Arrays.asList("insert"));
        match.put("operationType", in);
        var pipeline = new ArrayList < Map<String, Object>>();
        pipeline.add(UtilsMap.of("$match", match));
        ChangeStreamMonitor cm = new ChangeStreamMonitor(morphium, getCollectionName() + "_" + n, true, morphium.getConfig().connectionSettings().getMaxWaitTime(),
            pipeline);
        cm.addListener((evt)-> {

            Msg doc = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());
            if (doc.isExclusive()) {
                //try to get lock
                if (!lockMessage(doc, getSenderId())) {
                    //could not get Lock
                    //
                    return running.get();
                }
            }
            if(l.markAsProcessedBeforeExec()) {
                updateProcessedBy(doc);
            }
            try{
                var ret = l.onMessage(this, doc);
                if (doc.isDeleteAfterProcessing()) {
                    if (doc.getDeleteAfterProcessingTime() == 0) {
                        morphium.delete(doc);
                    }
                    else {
                        morphium.setInEntity(doc, getCollectionName(doc), Map.of("delte_at", new Date(System.currentTimeMillis() + doc.getDeleteAfterProcessingTime())), null);
                        // doc.setDeleteAt(new Date(System.currentTimeMillis() + doc.getDeleteAfterProcessingTime()));
                    }
                }
                else {
                    if (!l.markAsProcessedBeforeExec()) {
                        updateProcessedBy(doc);
                    }
                }
                if (ret != null) {
                    //send answer
                    ret.setInAnswerTo(doc.getMsgId());
                    ret.setRecipients(List.of(doc.getSender()));
                    sendMessage(ret);
                }
            } catch(Exception e) {
                if (e instanceof MessageRejectedException) {
                    deleteLock(doc.getMsgId());
                    log.warn("Message rejected", e);
                }
                else {
                    log.error("Error processing message", e);
                }
            }
            return running.get();
        });
        ChangeStreamMonitor lockMonitor = new ChangeStreamMonitor(morphium, getLockCollectionName(n), false, effectiveSettings.getMessagingPollPause(), List.of(Doc.of("$match", Doc.of("operationType",
            Doc.of("$eq", "delete")))));
        lockMonitor.addListener((evt)-> {
            return running.get();
        });
        monitorsByMsgName.putIfAbsent(n, new ArrayList<>());
        monitorsByMsgName.get(n).add(Map.of("monitor", cm, "listener", l));
        cm.start();
    }

    private void deleteLock(MorphiumId msgId) {
        morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName()).f("_id").eq(msgId).f("lock_id").eq(getSenderId()).remove();
    }

    @Override
    public void removeListenerForMessageNamed(String n, MessageListener l) {
        int idx = -1;

        for (var cm : monitorsByMsgName.get(n)) {
            idx++;

            if (cm.get("listener") == l) {
                break;
            }
        }

        if (idx >= 0) {
            ((ChangeStreamMonitor)monitorsByMsgName.get(n).get(idx).get("monitor")).terminate();
            monitorsByMsgName.get(n).remove(idx);
        }
    }
    @Override
    public String getSenderId() {
        return effectiveSettings.getSenderId();
    }
    @Override
    public MorphiumMessaging setSenderId(String id) {
        effectiveSettings.setSenderId(id);
        return this;
    }
    @Override
    public int getPause() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getPause'");
    }
    @Override
    public MorphiumMessaging setPause(int pause) {
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
    public MorphiumMessaging setAutoAnswer(boolean autoAnswer) {
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
    public MorphiumMessaging setProcessMultiple(boolean processMultiple) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setProcessMultiple'");
    }
    @Override
    public String getQueueName() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getQueueName'");
    }
    @Override
    public MorphiumMessaging setQueueName(String queueName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setQueueName'");
    }
    @Override
    public boolean isMultithreadded() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isMultithreadded'");
    }
    @Override
    public MorphiumMessaging setMultithreadded(boolean multithreadded) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setMultithreadded'");
    }
    @Override
    public int getWindowSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getWindowSize'");
    }
    @Override
    public MorphiumMessaging setWindowSize(int windowSize) {
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
    public MorphiumMessaging setPolling(boolean doPolling) {
        effectiveSettings.setUseChangeStrean(!doPolling);
        return this;
    }
    @Override
    public MorphiumMessaging setUseChangeStream(boolean useChangeStream) {
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
            effectiveSettings = overrides;
        }

        threadPool = new ThreadPoolExecutor(
            effectiveSettings.getThreadPoolMessagingCoreSize(),
            effectiveSettings.getThreadPoolMessagingMaxSize(),
            effectiveSettings.getThreadPoolMessagingKeepAliveTime(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            Thread.ofVirtual().name("msg-thr-", 0).factory()
        );
    }
}
