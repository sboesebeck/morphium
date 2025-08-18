package de.caluga.morphium.messaging;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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


    private enum MType {
        listener, monitor,
    }
    private Map < String, List< Map<MType, Object>>> monitorsByMsgName = new ConcurrentHashMap<>();
    private AtomicBoolean running = new AtomicBoolean(false);
    private String lockCollectionName;
    private static Vector<AdvancedSplitCollectionMessaging> allMessagings = new Vector<>();
    private Vector<String> pausedMessages = new Vector<>();
    private StatusInfoListener statusInfoListener = new StatusInfoListener();
    private String hostname = null;

    private Map<String, AtomicInteger> pollTrigger = new ConcurrentHashMap<>();

    private ScheduledThreadPoolExecutor decouplePool;
    public AdvancedSplitCollectionMessaging() {
        hostname = System.getenv("HOSTNAME");

        if (hostname == null) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException ignored) {
            }
        }

        if (hostname == null) {
            hostname = "unknown host";
        }

        decouplePool = new ScheduledThreadPoolExecutor(1, Thread.ofVirtual().name("decouple_thr-", 0).factory());

    }
    public List<MorphiumMessaging> getAlternativeMessagings() {
        return new Vector<>(allMessagings);
    }


    @Override
    public void start() {
        running.set(true);
        decouplePool.scheduleWithFixedDelay(()-> {
            for (var name : pollTrigger.keySet()) {
                if (pollTrigger.get(name).get() != 0) {
                    pollAndProcess(name);
                    pollTrigger.get(name).set(0);
                }
            }
        }, 1000, effectiveSettings.getMessagingPollPause(), TimeUnit.MILLISECONDS );
    }

    @Override
    public void enableStatusInfoListener() {
        effectiveSettings.setMessagingStatusInfoListenerEnabled(true);
        addListenerForMessageNamed(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
    }

    @Override
    public void disableStatusInfoListener() {
        effectiveSettings.setMessagingStatusInfoListenerEnabled(false);
        removeListenerForMessageNamed(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
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
        return effectiveSettings.isMessagingStatusInfoListenerEnabled();
    }

    @Override
    public void setStatusInfoListenerEnabled(boolean statusInfoListenerEnabled) {
        if (statusInfoListenerEnabled) {
            //avoid duplication
            if (!monitorsByMsgName.containsKey(effectiveSettings.getMessagingStatusInfoListenerName())
                    || !monitorsByMsgName.get(effectiveSettings.getMessagingStatusInfoListenerName()).contains(statusInfoListener)) {
                addListenerForMessageNamed(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
            }
        } else {
            removeListenerForMessageNamed(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
        }
    }

    @Override
    public Map<String, List<String>> getListenerNames() {
        Map<String, List<String>> ret = new HashMap<>();
        for (var e : monitorsByMsgName.entrySet()) {
            List<String> lst = new ArrayList<>();
            for (Map<MType, Object> l : e.getValue()) {
                MessageListener listener = (MessageListener)l.get(MType.listener);
                lst.add(listener.getClass().getName());
            }
            ret.put(e.getKey(), lst);
        }
        return ret;
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
        pausedMessages.add(name);
    }

    @Override
    public List<String> getPausedMessageNames() {
        return new ArrayList<String>(pausedMessages);
    }

    @Override
    public Long unpauseProcessingOfMessagesNamed(String name) {
        pausedMessages.remove(name);
        pollAndProcess(name);
        return 0L; //TODO: calculate time
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
        } else {
            //lock shall be deleted way after ttl or message
            lck.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl() * 1000 * 2));
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


    private void pollAndProcess(String msgName) {
        Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
        q1.f(Msg.Fields.exclusive).eq(true) //exclusive message
          .f(Msg.Fields.processedBy).eq(null) //not processed yet
          .f(Msg.Fields.sender).ne(getSenderId()); //not sent by me
        Query<Msg> q2 = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
        q2.f(Msg.Fields.exclusive).eq(false) //exclusive message
          .f(Msg.Fields.processedBy).ne(getSenderId()) //not processed by me
          .f(Msg.Fields.sender).ne(getSenderId()); //not sent by me

        Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
        q.sort(Msg.Fields.priority);
        q.or(q1, q2);
        for (Msg m : q.asIterable()) {
            for (var e : (List<Map<MType, Object>>)monitorsByMsgName.get(m.getName())) {
                var l = (MessageListener)e.get("listener");
                Runnable r = ()-> {
                    try{
                        if (m.isExclusive()) {
                            if (!lockMessage(m, getSenderId())) {
                                return;
                            }
                            //could lock message
                            // process exclusive message
                            //
                        }
                        if (l.markAsProcessedBeforeExec()) {
                            updateProcessedBy(m);
                        }
                        var ret = l.onMessage(this, m);
                        if (!l.markAsProcessedBeforeExec()) {
                            updateProcessedBy(m);
                        }

                        if (ret != null) {
                            ret.setSender(getSenderId());
                            ret.setRecipient(m.getSender());
                            ret.setInAnswerTo(m.getMsgId());
                            sendMessage(ret);
                        }
                    } catch (MessageRejectedException mre) {
                        log.warn("Message rejected");
                        updateProcessedBy(m);
                        unlock(m);
                    } catch(Throwable err) {
                        log.error("Error during message processing", e);
                    }
                };
                queueOrRun(r);

            }
        }

        //now look for broadcast messages
    }
    private void queueOrRun(Runnable r) {
        if (effectiveSettings.isMessagingMultithreadded()) {
            boolean queued = false;

            while (!queued) {
                try {
                    // throtteling to windowSize - do not create more threads than windowSize
                    while (threadPool.getActiveCount() > effectiveSettings.getMessagingWindowSize()) {
                        // log.debug(String.format("Active count %s > windowsize %s", threadPool.getActiveCount(), windowSize));
                        Thread.sleep(morphium.getConfig().driverSettings().getIdleSleepTime());
                    }

                    //                    log.debug(id+": Active count: "+threadPool.getActiveCount()+" / "+getWindowSize()+" - "+threadPool.getMaximumPoolSize());
                    threadPool.execute(r);
                    queued = true;
                } catch (Throwable ignored) {
                }
            }
        } else {
            r.run();
        }
    }

    private void pollAndProcess() {
        for (String name : monitorsByMsgName.keySet()) {
            pollAndProcess(name);
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
            Runnable r = ()->{
                if(l.markAsProcessedBeforeExec()) {
                    updateProcessedBy(doc);
                }
                try{
                    var ret = l.onMessage(this, doc);
                    if (doc.isDeleteAfterProcessing()) {
                        if (doc.getDeleteAfterProcessingTime() == 0) {
                            morphium.delete(doc);
                            if (doc.isExclusive()) {
                                unlock(doc);
                            }
                        } else {
                            morphium.setInEntity(doc, getCollectionName(doc), Map.of("delte_at", new Date(System.currentTimeMillis() + doc.getDeleteAfterProcessingTime())), null);
                            // doc.setDeleteAt(new Date(System.currentTimeMillis() + doc.getDeleteAfterProcessingTime()));
                        }
                    } else {
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
                } catch(MessageRejectedException mre) {
                    if(doc.isExclusive()) unlock(doc);
                    log.warn("Message rejected", mre);
                } catch(Exception e) {
                    if(doc.isExclusive()) unlock(doc);
                    log.error("Error processing message", e);
                }
            };
            queueOrRun(r);
            return running.get();
        });
        ChangeStreamMonitor lockMonitor = new ChangeStreamMonitor(morphium, getLockCollectionName(n), false, effectiveSettings.getMessagingPollPause(), List.of(Doc.of("$match", Doc.of("operationType",
            Doc.of("$eq", "delete")))));
        lockMonitor.addListener((evt)-> {
            var id = evt.getId();
            if (morphium.createQueryFor(Msg.class).setCollectionName(getCollectionName(n)).f(Msg.Fields.msgId).eq(id).countAll() != 0) {
                pollTrigger.putIfAbsent(n, new AtomicInteger());
                pollTrigger.get( n).incrementAndGet();
            }

            return running.get();
        });
        monitorsByMsgName.putIfAbsent(n, new ArrayList<>());
        monitorsByMsgName.get(n).add(Map.of(MType.monitor, cm, MType.listener, l));
        cm.start();
        pollAndProcess(n);
    }

    private void deleteLock(MorphiumId msgId) {
        morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName()).f("_id").eq(msgId).f("lock_id").eq(getSenderId()).remove();
    }

    private void unlock(Msg msg) {
        if (msg.isExclusive()) {
            deleteLock(msg.getMsgId());
        } //no need to release lock, if message was not locked
    }


    @Override
    public void removeListenerForMessageNamed(String n, MessageListener l) {
        int idx = -1;

        for (var cm : monitorsByMsgName.get(n)) {
            idx++;

            if (cm.get(MType.listener) == l) {
                break;
            }
        }

        if (idx >= 0) {
            ((ChangeStreamMonitor)monitorsByMsgName.get(n).get(idx).get(MType.monitor)).terminate();
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
        return effectiveSettings.getMessagingPollPause();
    }
    @Override
    public MorphiumMessaging setPause(int pause) {
        effectiveSettings.setMessagingPollPause(pause);
        return this;
    }
    @Override
    public boolean isRunning() {
        return running.get();
    }
    @Override
    public void terminate() {
        for (var e : monitorsByMsgName.entrySet()) {
            for (var m : e.getValue()) {
                ((ChangeStreamMonitor) m.get(MType.monitor)).terminate();;
            }
        }

        monitorsByMsgName.clear();
        running.set(false);
    }
    @Override
    public void addMessageListener(MessageListener l) {
        //global listener...
        throw new UnsupportedOperationException("Global listeners not supported");
    }
    @Override
    public void removeMessageListener(MessageListener l) {
        log.error("No support for global MessageListener");
    }
    @Override
    public void queueMessage(Msg m) {
        m.setSenderHost(hostname);
        m.setSender(getSenderId());
        morphium.store(m, getCollectionName(m), null);
    }
    @Override
    public void sendMessage(Msg m) {
        queueMessage(m);
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
        return effectiveSettings.isAutoAnswer();
    }
    @Override
    public MorphiumMessaging setAutoAnswer(boolean autoAnswer) {
        effectiveSettings.setAutoAnswer(autoAnswer);
        return this;
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
        return effectiveSettings.isProcessMultiple();
    }
    @Override
    public MorphiumMessaging setProcessMultiple(boolean processMultiple) {
        effectiveSettings.setProcessMultiple(processMultiple);
        return this;
    }
    @Override
    public String getQueueName() {
        return effectiveSettings.getMessageQueueName();
    }
    @Override
    public MorphiumMessaging setQueueName(String queueName) {
        effectiveSettings.setMessageQueueName(queueName);
        return this;
    }
    @Override
    public boolean isMultithreadded() {
        return effectiveSettings.isMessagingMultithreadded();
    }
    @Override
    public MorphiumMessaging setMultithreadded(boolean multithreadded) {
        effectiveSettings.setMessagingMultithreadded(multithreadded);
        return this;
    }
    @Override
    public int getWindowSize() {
        return effectiveSettings.getMessagingWindowSize();
    }
    @Override
    public MorphiumMessaging setWindowSize(int windowSize) {
        effectiveSettings.setMessagingWindowSize(windowSize);
        return this;
    }
    @Override
    public boolean isUseChangeStream() {
        return morphium.getConfig().clusterSettings().isReplicaset() && effectiveSettings.isUseChangeStrean();
    }
    @Override
    public int getRunningTasks() {
        return threadPool.getActiveCount();
    }
    @Override
    public Morphium getMorphium() {
        return morphium;
    }
    @Override
    public MorphiumMessaging setPolling(boolean doPolling) {
        effectiveSettings.setUseChangeStrean(!doPolling);
        return this;
    }
    @Override
    public MorphiumMessaging setUseChangeStream(boolean useChangeStream) {
        effectiveSettings.setUseChangeStrean(useChangeStream);
        return this;
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
