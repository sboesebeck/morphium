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
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
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
import de.caluga.morphium.messaging.StdMessaging.MessageTimeoutException;
import de.caluga.morphium.messaging.StdMessaging.SystemShutdownException;
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

    private final Map<MorphiumId, Queue<Msg>> waitingForAnswers = new ConcurrentHashMap<>();
    private final Map<MorphiumId, CallbackRequest> waitingForCallbacks = new ConcurrentHashMap<>();

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
    private class CallbackRequest {
        Msg theMessage;
        AsyncMessageCallback callback;
        long ttl;
        long timestamp;
    }

    private AsyncOperationCallback aCallback = new AsyncOperationCallback<Msg>() {

        @Override
        public void onOperationError(AsyncOperationType type, Query q,
                                     long duration, String error, Throwable t, Msg entity, Object... param) {
            log.error("Could not store {}", error, t);
        }

        @Override
        public void onOperationSucceeded(AsyncOperationType type, Query<Msg> q,
                                         long duration, List<Msg> result, Msg entity,
                                         Object... param) {

        }
    };
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
        return threadPool.getActiveCount();
    }

    @Override
    public int getInProgressCount() {
        return getProcessingCount();
    }

    @Override
    public int waitingForAnswersCount() {
        return waitingForAnswers.size();
    }

    @Override
    public int waitingForAnswersTotalCount() {
        int cnt = 0;

        for (Queue l : waitingForAnswers.values()) {
            cnt = cnt + l.size();
        }

        return cnt;
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
        log.warn("Globallisteners not supported");
        return List.of();
    }

    @Override
    public Map<String, Long> getThreadPoolStats() {
        if (threadPool == null) return Map.of();

        String prefix = "messaging.threadpool.";
        return UtilsMap.of(prefix + "largest_poolsize", Long.valueOf(threadPool.getLargestPoolSize())).add(prefix + "task_count", threadPool.getTaskCount())
               .add(prefix + "core_size", (long) threadPool.getCorePoolSize()).add(prefix + "maximum_pool_size", (long) threadPool.getMaximumPoolSize())
               .add(prefix + "pool_size", (long) threadPool.getPoolSize()).add(prefix + "active_count", (long) threadPool.getActiveCount())
               .add(prefix + "completed_task_count", threadPool.getCompletedTaskCount());
    }

    @Override
    public long getPendingMessagesCount() {
        long sum = 0;
        for (var msgName : monitorsByMsgName.keySet()) {
            Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
            q1.f(Msg.Fields.sender).ne(getSenderId()).f("processed_by.0").notExists();
            sum += q1.countAll();
        }
        return sum;
    }

    @Override
    public void removeMessage(Msg m) {
        morphium.delete(m, getCollectionName(m));
    }


    @Override
    public int getAsyncMessagesPending() {
        return waitingForCallbacks.size();
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
                        //Check answers
                        if (m.isAnswer()) {
                            final Queue<Msg> answersForMessage = waitingForAnswers.get(m.getInAnswerTo());

                            if (null != answersForMessage) {
                                // we're expecting this message!
                                updateProcessedBy(m);

                                if (!answersForMessage.contains(m)) {
                                    answersForMessage.add(m);
                                }

                                checkDeleteAfterProcessing(m);
                                return;
                            }

                            final CallbackRequest cbr = waitingForCallbacks.get(m.getInAnswerTo());
                            final Msg theMessage = m;

                            if (cbr != null) {
                                AsyncMessageCallback cb = cbr.callback;
                                Runnable cbRunnable = () -> {
                                    cb.incomingMessage(theMessage);
                                };
                                updateProcessedBy(theMessage);
                                queueOrRun(cbRunnable);

                                if (cbr.theMessage.isExclusive()) {
                                    waitingForCallbacks.remove(m.getInAnswerTo());
                                }

                                if (m.isDeleteAfterProcessing()) {
                                    checkDeleteAfterProcessing(m);
                                }
                                return;
                            }
                        }

                        if (m.getRecipients() != null && !m.getRecipients().isEmpty() && !m.getRecipients().contains(getSenderId())) {
                            //message not for me
                            return;
                        }

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
                        checkDeleteAfterProcessing(m);

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
                    } catch (Throwable err) {
                        log.error("Error during message processing", e);
                    }
                };
                queueOrRun(r);

            }
        }

        //now look for broadcast messages
    }

    private void checkDeleteAfterProcessing(Msg obj) {
        if (obj.isDeleteAfterProcessing()) {
            if (obj.getDeleteAfterProcessingTime() == 0) {
                morphium.delete(obj, getCollectionName());
                if (obj.isExclusive()) unlock(obj);
            } else {
                obj.setDeleteAt(new Date(System.currentTimeMillis() + obj.getDeleteAfterProcessingTime()));
                morphium.setInEntity(obj, getCollectionName(), Msg.Fields.deleteAt, obj.getDeleteAt());

                if (obj.isExclusive()) {
                    morphium.createQueryFor(MsgLock.class, getLockCollectionName()).f("_id").eq(obj.getMsgId()).set(MsgLock.Fields.deleteAt, obj.getDeleteAt());
                }
            }
        }
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
            if (doc.getRecipients() != null && !doc.getRecipients().isEmpty() && !doc.getRecipients().contains(getSenderId())) {
                //message not for me
                return running.get();
            }
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
                        checkDeleteAfterProcessing(doc);
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
        morphium.store(m, getCollectionName(m), aCallback);
    }
    @Override
    public void sendMessage(Msg m) {
        m.setSenderHost(hostname);
        m.setSender(getSenderId());
        morphium.store(m, getCollectionName(m), null);
    }
    @Override
    public long getNumberOfMessages() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getNumberOfMessages'");
    }
    @Override
    public void sendMessageToSelf(Msg m) {

        m.setSender("self");
        m.setRecipient(getSenderId());
        morphium.store(m, getCollectionName(m), null);
    }
    @Override
    public void queueMessagetoSelf(Msg m) {

        m.setSender("self");
        m.setRecipient(getSenderId());
        morphium.store(m, getCollectionName(m), aCallback );
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
    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs, boolean throwExceptionOnTimeout) {
        if (!running.get()) {
            throw new SystemShutdownException("Messaging shutting down - abort sending!");
        }

        if (theMessage.getMsgId() == null)
            theMessage.setMsgId(new MorphiumId());

        final MorphiumId requestMsgId = theMessage.getMsgId();
        final LinkedBlockingDeque<Msg> blockingQueue = new LinkedBlockingDeque<>();
        waitingForAnswers.put(requestMsgId, blockingQueue);

        try {
            sendMessage(theMessage);
            T firstAnswer = (T) blockingQueue.poll(timeoutInMs, TimeUnit.MILLISECONDS);

            if (null == firstAnswer && throwExceptionOnTimeout) {
                throw new MessageTimeoutException("Did not receive answer for message " + theMessage.getName() + "/" + requestMsgId + " in time (" + timeoutInMs + "ms)");
            }

            return firstAnswer;
        } catch (InterruptedException e) {
            log.error("Did not receive answer for message " + theMessage.getName() + "/" + requestMsgId + " interrupted.", e);
        } finally {
            waitingForAnswers.remove(requestMsgId);
        }

        return null;
    }

    @Override
    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout) {
        return sendAndAwaitAnswers(theMessage, numberOfAnswers, timeout, false);
    }

    @Override
    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout, boolean throwExceptionOnTimeout) {
        MorphiumId requestMsgId = theMessage.getMsgId();

        if (requestMsgId == null) {
            theMessage.setMsgId(new MorphiumId());
            requestMsgId = theMessage.getMsgId();
        }

        final Queue<Msg> answerList = new LinkedBlockingDeque<>();
        waitingForAnswers.put(requestMsgId, answerList);
        sendMessage(theMessage);
        long start = System.currentTimeMillis();
        List<T> returnValue = null;

        try {
            while (running.get()) {
                if (answerList.size() > 0) {
                    if (numberOfAnswers > 0 && answerList.size() >= numberOfAnswers) {
                        break;
                    }

                    // time up - return all answers that were received
                }

                // Did not receive any message in time
                if (throwExceptionOnTimeout && System.currentTimeMillis() - start > timeout && (answerList.isEmpty())) {
                    throw new MessageTimeoutException("Did not receive any answer for message " + theMessage.getName() + "/" + requestMsgId + "in time (" + timeout + ")");
                }

                if (System.currentTimeMillis() - start > timeout) {
                    break;
                }

                if (!running.get()) {
                    throw new SystemShutdownException("Messaging shutting down - abort waiting!");
                }

                try {
                    Thread.sleep(morphium.getConfig().driverSettings().getIdleSleepTime());
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } finally {
            returnValue = new ArrayList(waitingForAnswers.remove(requestMsgId));
        }

        return returnValue;
    }


    @Override
    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs) {
        return sendAndAwaitFirstAnswer(theMessage, timeoutInMs, true);
    }

    /**
    * Sends a message asynchronously and sends all incoming answers via callback.
    * If sent message is exclusive, only one answer will be processed, otherwise all incoming answers up to timeout
    * will be processed.
    *
    * @parameter theMessage to be sent
    * @parameter timoutInMs - milliseconds to wait until listener is removed
    * @parameter cb - the message callback
    */
    @Override
    public <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs, AsyncMessageCallback cb) {
        if (!running.get()) {
            throw new SystemShutdownException("Messaging shutting down - abort sending!");
        }

        if (theMessage.getMsgId() == null)
            theMessage.setMsgId(new MorphiumId());

        final MorphiumId requestMsgId = theMessage.getMsgId();
        final CallbackRequest cbr = new CallbackRequest();
        cbr.timestamp = System.currentTimeMillis();
        cbr.theMessage = theMessage;
        cbr.callback = cb;
        cbr.ttl = timeoutInMs;
        waitingForCallbacks.put(requestMsgId, cbr);
        sendMessage(theMessage);
        decouplePool.schedule(() -> {
            waitingForCallbacks.remove(requestMsgId);
        }, timeoutInMs, TimeUnit.MILLISECONDS);
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
        return morphium.getConfig().clusterSettings().isReplicaset() && effectiveSettings.isUseChangeStream();
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
        effectiveSettings.setUseChangeStream(!doPolling);
        return this;
    }
    @Override
    public MorphiumMessaging setUseChangeStream(boolean useChangeStream) {
        effectiveSettings.setUseChangeStream(useChangeStream);
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
