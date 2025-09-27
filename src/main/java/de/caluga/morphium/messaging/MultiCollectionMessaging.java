
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
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Messaging;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.messaging.SingleCollectionMessaging.AsyncMessageCallback;
import de.caluga.morphium.messaging.SingleCollectionMessaging.MessageTimeoutException;
import de.caluga.morphium.messaging.SingleCollectionMessaging.SystemShutdownException;
import de.caluga.morphium.query.Query;

/**
 * MessageQueueing implementation with additional features:
 * - using a collection for each message name - reducing load on clients with
 * minimal overhead on MongoDB-Side
 * - adding capability for streaming data. Usually as an answer, but could also
 * be as "just as is"
 */
@Messaging(name = "MultiCollectionMessaging", description = "Advanced multi-collection messaging implementation")
public class MultiCollectionMessaging implements MorphiumMessaging {

    private Logger log = LoggerFactory.getLogger(MultiCollectionMessaging.class);
    private Morphium morphium;
    private MessagingSettings effectiveSettings;
    private ThreadPoolExecutor threadPool;
    private Set<MorphiumId> processingMessages = ConcurrentHashMap.newKeySet();

    private final Map<MorphiumId, Queue<Msg>> waitingForAnswers = new ConcurrentHashMap<>();
    private final Map<MorphiumId, CallbackRequest> waitingForCallbacks = new ConcurrentHashMap<>();

    private enum MType {
        listener, monitor, lockMonitor,
    }

    private Map<String, List<Map<MType, Object>>> monitorsByTopic = new ConcurrentHashMap<>();
    private AtomicBoolean running = new AtomicBoolean(false);
    private String lockCollectionName;
    private static Vector<MultiCollectionMessaging> allMessagings = new Vector<>();
    private Set<String> pausedTopics = ConcurrentHashMap.newKeySet();
    private StatusInfoListener statusInfoListener = new StatusInfoListener();
    private String hostname = null;
    private String senderId;

    private Map<String, AtomicInteger> pollTrigger = new ConcurrentHashMap<>();
    // track when a topic was paused, to report elapsed pause time on unpause
    private final Map<String, Long> pausedAt = new ConcurrentHashMap<>();

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
    private ChangeStreamMonitor directMessagesMonitor;

    public MultiCollectionMessaging() {
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
        allMessagings.add(this);

    }

    public List<MorphiumMessaging> getAlternativeMessagings() {
        return new Vector<>(allMessagings);
    }

    public String getDMCollectionName() {
        return getDMCollectionName(getSenderId());
    }

    @Override
    public String getDMCollectionName(String sender) {
        return "dm_" + morphium.getARHelper().createCamelCase(sender, false);
    }

    @Override
    public void start() {
        running.set(true);
        decouplePool.scheduleWithFixedDelay(() -> {
            // Process poll triggers - always handle DMs, regular messages based on change stream status
            for (var name : pollTrigger.keySet()) {
                if (pollTrigger.get(name).get() != 0) {
                    if (name.startsWith("dm_")) {
                        pollAndProcessDms(name.substring(3));
                    } else if (!isUseChangeStream()) {
                        // Only poll when change streams are disabled
                        pollAndProcess(name);
                    }
                    pollTrigger.get(name).set(0);
                }
            }
            Map<MorphiumId, CallbackRequest> cp = new HashMap<>(waitingForCallbacks); // copy to avoid concurrent
            // Modification in loop
            for (var entry : cp.entrySet()) {
                if (System.currentTimeMillis() - entry.getValue().timestamp > entry.getValue().ttl) {
                    // callback timed out
                    decouplePool.schedule(() -> {
                        waitingForCallbacks.remove(entry.getKey());
                    }, 1000, TimeUnit.MILLISECONDS);
                }
            }

        }, 1000, effectiveSettings.getMessagingPollPause(), TimeUnit.MILLISECONDS);

        String dmCollectionName = getDMCollectionName();
        morphium.ensureIndicesFor(Msg.class, dmCollectionName);

        List<Map<String, Object>> pipeline = List
                                             .of(Doc.of("$match", Doc.of("operationType", Doc.of("$eq", "insert"))));
        directMessagesMonitor = new ChangeStreamMonitor(morphium, dmCollectionName, true,
            morphium.getConfig().connectionSettings().getMaxWaitTime(), pipeline);
        directMessagesMonitor.addListener((evt) -> {
            // All messages coming in here are for me!
            Map<String, Object> doc = evt.getFullDocument();
            Msg msg = morphium.getMapper().deserialize(Msg.class, doc);
            if (msg.isAnswer()) {
                // Answer handling
                handleAnswer(msg);
            } else {
                if (pausedTopics.contains(msg.getTopic())) {
                    return running.get();

                }
                if (monitorsByTopic.containsKey(msg.getTopic())) {
                    // Use atomic add operation - if already present, skip processing
                    if (!processingMessages.add(msg.getMsgId())) {
                        return running.get();
                    }
                    for (var map : monitorsByTopic.get(msg.getTopic())) {
                        MessageListener l = (MessageListener) map.get(MType.listener);
                        queueOrRun(() -> {
                            if (l.markAsProcessedBeforeExec()) {
                                updateProcessedBy(msg);
                            }
                            try {
                                var ret = l.onMessage(MultiCollectionMessaging.this, msg);
                                if (!running.get())
                                    return;
                                if (ret == null && effectiveSettings.isAutoAnswer()) {
                                    ret = new Msg(msg.getTopic(), "received", "");
                                }
                                if (ret != null) {
                                    ret.setRecipient(msg.getSender());
                                    ret.setInAnswerTo(msg.getMsgId());
                                    queueMessage(ret);
                                }
                            } catch (Exception e) {
                                log.error("Error processinig message");
                            }

                            var deleted = false;
                            if (msg.isDeleteAfterProcessing()) {
                                if (msg.getDeleteAfterProcessingTime() == 0) {
                                    morphium.remove(msg, dmCollectionName);
                                    deleted = true;
                                } else {
                                    msg.setDeleteAt(
                                                    new Date(System.currentTimeMillis() + msg.getDeleteAfterProcessingTime()));
                                    msg.addProcessedId(getSenderId());
                                    morphium.updateUsingFields(msg, dmCollectionName, new AsyncCallbackAdapter(),
                                                               Msg.Fields.deleteAt, Msg.Fields.processedBy);
                                }
                            }
                            if (!deleted && !l.markAsProcessedBeforeExec()) {
                                updateProcessedBy(msg);
                            }
                            processingMessages.remove(msg.getMsgId());

                        });
                    }
                } else {
                    log.warn("incoming direct message for topic {} - no listener registered", msg.getTopic());
                    morphium.remove(msg, dmCollectionName);
                }
            }
            return running.get();
        });
        directMessagesMonitor.start();
        if (!isUseChangeStream()) {
            log.info("Start polling as changestreams are disabled");
            decouplePool.scheduleWithFixedDelay(()-> {
                try {
                    pollAndProcess();
                } catch (Throwable e) {
                    log.info("Error in polling thread", e);
                }
            }, 1000, getPause(), TimeUnit.MILLISECONDS);
        } else {
            pollAndProcess();
        }
    }

    @Override
    public void enableStatusInfoListener() {
        effectiveSettings.setMessagingStatusInfoListenerEnabled(true);
        addListenerForTopic(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
    }

    @Override
    public void disableStatusInfoListener() {
        effectiveSettings.setMessagingStatusInfoListenerEnabled(false);
        removeListenerForTopic(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
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
            // avoid duplication
            if (!monitorsByTopic.containsKey(effectiveSettings.getMessagingStatusInfoListenerName())
                    || !monitorsByTopic.get(effectiveSettings.getMessagingStatusInfoListenerName())
                    .contains(statusInfoListener)) {
                addListenerForTopic(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
            }
        } else {
            removeListenerForTopic(effectiveSettings.getMessagingStatusInfoListenerName(), statusInfoListener);
        }
    }

    @Override
    public Map<String, List<String>> getListenerNames() {
        Map<String, List<String>> ret = new HashMap<>();
        for (var e : monitorsByTopic.entrySet()) {
            List<String> lst = new ArrayList<>();
            for (Map<MType, Object> l : e.getValue()) {
                MessageListener listener = (MessageListener) l.get(MType.listener);
                lst.add(listener.getClass().getName());
            }
            ret.put(e.getKey(), lst);
        }
        return ret;
    }

    @Override
    public Map<String, Long> getThreadPoolStats() {
        if (threadPool == null)
            return Map.of();

        String prefix = "messaging.threadpool.";
        return UtilsMap.of(prefix + "largest_poolsize", Long.valueOf(threadPool.getLargestPoolSize()))
               .add(prefix + "task_count", threadPool.getTaskCount())
               .add(prefix + "core_size", (long) threadPool.getCorePoolSize())
               .add(prefix + "maximum_pool_size", (long) threadPool.getMaximumPoolSize())
               .add(prefix + "pool_size", (long) threadPool.getPoolSize())
               .add(prefix + "active_count", (long) threadPool.getActiveCount())
               .add(prefix + "completed_task_count", threadPool.getCompletedTaskCount());
    }

    @Override
    public long getPendingMessagesCount() {
        long sum = 0;
        for (var msgName : monitorsByTopic.keySet()) {
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
    public void pauseTopicProcessing(String name) {
        pausedTopics.add(name);
        pausedAt.putIfAbsent(name, System.currentTimeMillis());
    }

    @Override
    public List<String> getPausedTopics() {
        return new ArrayList<String>(pausedTopics);
    }

    @Override
    public Long unpauseTopicProcessing(String name) {
        pausedTopics.remove(name);
        Long started = pausedAt.remove(name);
        decouplePool.execute(() -> {
            if (!isUseChangeStream()) {
                pollAndProcess(name);
            }
            pollAndProcessDms(name);
        });
        if (started == null) return 0L;
        return System.currentTimeMillis() - started;
    }

    @Override
    public String getCollectionName() {
        return effectiveSettings.getMessageQueueName();
    }

    @Override
    public String getCollectionName(Msg m) {
        return getCollectionName(m.getTopic());
    }

    @Override
    public String getCollectionName(String n) {
        return (getCollectionName() + "_" + n).replaceAll(" ", "").replaceAll("-", "").replaceAll("/", "");
    }

    private MsgLock getLock(Msg m) {
        return morphium.findById(MsgLock.class, m.getMsgId(), getLockCollectionName() + "_" + m.getTopic());
    }

    @Override
    public String getLockCollectionName() {
        if (lockCollectionName == null) {
            lockCollectionName = getCollectionName() + "_lck";
        }

        return lockCollectionName;
    }

    @Override
    public String getLockCollectionName(Msg m) {
        return getLockCollectionName(m.getTopic());
    }

    @Override
    public String getLockCollectionName(String name) {
        // TODO: improve
        return (getLockCollectionName() + "_" + name).replaceAll(" ", "").replaceAll("-", "").replaceAll("/", "");
    }

    private boolean lockMessage(Msg m, String lockId) {
        return lockMessage(m, lockId, null);
    }

    // private boolean lockMessage(MorphiumId id, String msgName, String lockId) {
    //     MsgLock lck = new MsgLock(id);
    //     lck.setLockId(lockId);
    //     lck.setDeleteAt(new Date(System.currentTimeMillis() + 600000));
    //     InsertMongoCommand cmd = null;

    //     try {
    //         cmd = new InsertMongoCommand(
    //                         morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(MsgLock.class)));
    //         cmd.setColl(getLockCollectionName(msgName)).setDb(morphium.getDatabase())
    //            .setDocuments(List.of(morphium.getMapper().serialize(lck)));
    //         cmd.execute();
    //         return true;
    //     } catch (Exception e) {
    //         return false;
    //     } finally {
    //         if (cmd != null) {
    //             cmd.releaseConnection();
    //         }
    //     }
    // }

    public boolean lockMessage(Msg m, String lockId, Date delAt) {
        MsgLock lck = new MsgLock(m);
        lck.setLockId(lockId);

        if (delAt != null) {
            lck.setDeleteAt(delAt);
        } else {
            // lock shall be deleted way after ttl or message
            lck.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl() * 2));
        }

        InsertMongoCommand cmd = null;

        try {
            cmd = new InsertMongoCommand(
                            morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(MsgLock.class)));
            cmd.setColl(getLockCollectionName(m)).setDb(morphium.getDatabase())
               .setDocuments(List.of(morphium.getMapper().serialize(lck)));
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

    private void handleAnswer(Msg m) {
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

        if (pausedTopics.contains(m.getTopic()))
            return;
        final CallbackRequest cbr = waitingForCallbacks.get(m.getInAnswerTo());
        final Msg theMessage = m;

        if (cbr != null) {
            AsyncMessageCallback cb = cbr.callback;
            Runnable cbRunnable = () -> {
                cb.incomingMessage(theMessage);
                waitingForCallbacks.remove(m.getInAnswerTo());
            };
            queueOrRun(cbRunnable);
        } else {
            // an answer, but no one is waiting for it
            processMessage(theMessage);
        }
    }

    private void processMessage(Msg m) {
        if (!monitorsByTopic.containsKey(m.getTopic())) {
            // log.error("I {} Got a message I do not have a listener configured for: {}!",
            // getSenderId(), m.getName());
            return; // cannot process message, as there is no listener? HOW?
        }
        for (var e : (List<Map<MType, Object>>) monitorsByTopic.get(m.getTopic())) {
            var l = (MessageListener) e.get(MType.listener);
            if (l == null)
                continue;

            // Use atomic add operation - if already present, skip processing
            if (!processingMessages.add(m.getMsgId())) {
                return;
            }
            Runnable r = () -> {
                Msg current = morphium.reread(m, getCollectionName(m));
                if (current == null) {
                    processingMessages.remove(m.getMsgId());
                    return;
                }

                try {
                    if (current.getProcessedBy().contains(getSenderId())) {
                        // Don't unlock here - let lock timeout naturally
                        return;
                    }

                    if (pausedTopics.contains(current.getTopic()))
                        return;

                    if (l.markAsProcessedBeforeExec()) {
                        updateProcessedBy(current);
                    }
                    var ret = l.onMessage(this, current);
                    if (!running.get())
                        return;
                    if (effectiveSettings.isAutoAnswer() && ret == null) {
                        ret = new Msg(current.getTopic(), "received", "");

                    }
                    if (!checkDeleteAfterProcessing(current) && !l.markAsProcessedBeforeExec()) {
                        updateProcessedBy(current);
                    }

                    if (ret != null) {
                        ret.setSender(getSenderId());
                        ret.setRecipient(current.getSender());
                        ret.setInAnswerTo(current.getMsgId());
                        sendMessage(ret);
                    }
                } catch (MessageRejectedException mre) {
                    log.warn("Message rejected");
                    updateProcessedBy(current);
                    unlock(current);
                } catch (Throwable err) {
                    log.error("Error during message processing", err);
                    unlock(current);
                } finally {
                    processingMessages.remove(m.getMsgId());
                }
            };
            queueOrRun(r);

        }
    }

    private void pollAndProcessDms(String name) {
        // TODO: look for paused dms
        //
        //
        var q = morphium.createQueryFor(Msg.class, getDMCollectionName()).f(Msg.Fields.processedBy).eq(null) // not
                // processed
                .f(Msg.Fields.topic).eq(name).f(Msg.Fields.msgId).nin(new ArrayList(processingMessages));
        int window = getWindowSize();
        q.limit(window + 1);
        int seen = 0;
        boolean more = false;
        for (Msg m : q.asIterable(window + 1)) {
            if (seen >= window) {
                more = true;
                break;
            }
            for (var e : (List<Map<MType, Object>>) monitorsByTopic.get(m.getTopic())) {
                var l = (MessageListener) e.get(MType.listener);
                queueOrRun(() -> {
                    if (m.isAnswer()) {
                        handleAnswer(m);
                    } else {
                        processMessage(m);
                    }
                });
            }
            seen++;
        }
        if (more) {
            pollTrigger.putIfAbsent("dm_" + name, new AtomicInteger(0));
            pollTrigger.get("dm_" + name).incrementAndGet();
        }
    }

    private void pollAndProcess(String msgName) {
        if (!running.get())
            return;
        // log.info("PollAndProcess - ignoring {} msids", processingMessages.size());
        // Use more efficient query patterns
        List<MorphiumId> processingIds = new ArrayList<>(processingMessages);

        Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
        q1.f(Msg.Fields.exclusive).eq(true) // exclusive message
          .f("processed_by.0").notExists() // not processed yet (more efficient than eq(null))
          .f(Msg.Fields.sender).ne(getSenderId()); // not sent by me
        if (!processingIds.isEmpty()) {
            q1.f(Msg.Fields.msgId).nin(processingIds); // not processed by me
        }

        Query<Msg> q2 = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
        q2.f(Msg.Fields.exclusive).eq(false) // non-exclusive message
          .f(Msg.Fields.processedBy).ne(getSenderId()) // not processed by me
          .f(Msg.Fields.sender).ne(getSenderId()); // not sent by me
        if (!processingIds.isEmpty()) {
            q2.f(Msg.Fields.msgId).nin(processingIds); // not processing already
        }

        Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
        q.sort(Msg.Fields.priority);
        q.or(q1, q2);
        int window = getWindowSize();
        q.limit(window + 1);
        if (!running.get())
            return;
        int seen = 0;
        boolean more = false;
        for (Msg m : q.asIterable(window + 1)) {
            if (seen >= window) {
                more = true;
                break;
            }
            if (m.isTimingOut() && System.currentTimeMillis() - m.getTimestamp() > m.getTtl()) {
                log.debug("deleting outdated message");
                morphium.delete(m);
                return;
            }
            if (pausedTopics.contains(m.getTopic())) {
                // paused
                return;
            }
            // Check answers
            if (m.isAnswer()) { // should never come in this collection!!! TODO
                handleAnswer(m);
            } else {
                // happens if pausing is enabled during processing!
                if (pausedTopics.contains(m.getTopic())) {
                    continue;
                }
                if (m.getRecipients() != null && !m.getRecipients().isEmpty()
                        && !m.getRecipients().contains(getSenderId())) {
                    // message not for me
                    // TODO: should never be here as we use DM_collections
                    return;
                }

                if (m.isExclusive()) {
                    if (!lockMessage(m, getSenderId())) {
                        return;
                    }
                    // could lock message
                    // process exclusive message
                    //
                }
                processMessage(m);
            }
            seen++;
        }
        if (more) {
            pollTrigger.putIfAbsent(msgName, new AtomicInteger(0));
            pollTrigger.get(msgName).incrementAndGet();
        }

    }

    private boolean checkDeleteAfterProcessing(Msg message) {
        if (message.isDeleteAfterProcessing()) {
            if (message.getDeleteAfterProcessingTime() == 0) {
                morphium.delete(message, getCollectionName(message));
                // unlock(message);
                return true;
            } else {
                message.setDeleteAt(new Date(System.currentTimeMillis() + message.getDeleteAfterProcessingTime()));
                morphium.setInEntity(message, getCollectionName(message), Msg.Fields.deleteAt, new Date(System.currentTimeMillis() + message.getDeleteAfterProcessingTime()));

                // if (message.getDeleteAfterProcessingTime() > 0 && morphium.getDriver() instanceof de.caluga.morphium.driver.inmem.InMemoryDriver) {
                //     long delay = Math.max(message.getDeleteAfterProcessingTime(), 10_000);
                //     java.util.concurrent.CompletableFuture.runAsync(() -> {
                //         try {
                //             morphium.createQueryFor(Msg.class, getCollectionName(message))
                //                     .f(Msg.Fields.msgId).eq(message.getMsgId()).remove();
                //         } catch (Exception e) {
                //             log.warn("Failed to remove message after processing", e);
                //         }
                //     }, java.util.concurrent.CompletableFuture.delayedExecutor(delay, java.util.concurrent.TimeUnit.MILLISECONDS));
                // }

                if (message.isExclusive()) {
                    morphium.createQueryFor(MsgLock.class, getLockCollectionName(message)).f("_id")
                            .eq(message.getMsgId())
                            .set(MsgLock.Fields.deleteAt, new Date(System.currentTimeMillis() + message.getDeleteAfterProcessingTime()));
                }
            }
        }
        return false;
    }

    private void queueOrRun(Runnable r) {
        if (effectiveSettings.isMessagingMultithreadded()) {
            boolean queued = false;

            while (!queued) {
                try {
                    // throtteling to windowSize - do not create more threads than windowSize
                    while (threadPool.getActiveCount() > effectiveSettings.getMessagingWindowSize()) {
                        Thread.onSpinWait(); // CPU-friendly busy wait hint
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS
                                              .toNanos(morphium.getConfig().driverSettings().getIdleSleepTime())); // log.debug(String.format("Active
                        // count %s >
                        // windowsize %s",
                        // threadPool.getActiveCount(),
                        // windowSize));
                        // Thread.sleep(morphium.getConfig().driverSettings().getIdleSleepTime());
                    }

                    // log.debug(id+": Active count: "+threadPool.getActiveCount()+" /
                    // "+getWindowSize()+" - "+threadPool.getMaximumPoolSize());
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
        for (String name : monitorsByTopic.keySet()) {
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
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName(msg)).setDb(morphium.getDatabase());
            cmd.addUpdate(qobj, update, null, false, false, null, null, null);
            if (!running.get())
                return; // this happens during tests mainly
            Map<String, Object> ret = cmd.execute();
            cmd.releaseConnection();
            cmd = null;

            // log.debug("Updating processed by for "+id+" on message "+msg.getMsgId());
            // if (ret.get("nModified") == null && ret.get("modified") == null
            //         || Integer.valueOf(0).equals(ret.get("nModified"))) {
            //     if (morphium.reread(msg, getCollectionName(msg)) != null) {
            //         if (!msg.getProcessedBy().contains(id)) {
            //             log.warn(id + ": Could not update processed_by in msg " + msg.getMsgId());
            //             log.warn(id + ": " + Utils.toJsonString(ret));
            //             log.warn(id + ": msg: " + msg.toString());
            //         }

            //         // } else {
            //         // log.debug("message deleted by someone else!!!");
            //     }
            // }
        } catch (MorphiumDriverException e) {
            log.error("Error updating processed by - this might lead to duplicate execution!", e);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    @Override
    public void addListenerForTopic(String n, MessageListener l) {
        Map<String, Object> match = new LinkedHashMap<>();
        Map<String, Object> in = new LinkedHashMap<>();
        // in.put("$eq", "insert"); //, "delete", "update"));
        in.put("$in", Arrays.asList("insert"));
        match.put("operationType", in);
        match.put("full_document.sender", Map.of("$ne", getSenderId()));
        var pipeline = new ArrayList<Map<String, Object>>();
        pipeline.add(UtilsMap.of("$match", match));
        log.debug("Adding changestream for collection {}", getCollectionName(n));
        morphium.ensureIndicesFor(Msg.class, getCollectionName(n));
        ChangeStreamMonitor cm = new ChangeStreamMonitor(morphium, getCollectionName(n), true,
            morphium.getConfig().connectionSettings().getMaxWaitTime(),
            pipeline);
        cm.addListener((evt) -> {
            Runnable r = () -> {

                try {
                    // Msg doc = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());
                    Map<String, Object> map = evt.getFullDocument();
                    // if (getSenderId().equals(map.get(Msg.Fields.sender.name()))) return; //own
                    // message

                    // this part is obsolete - recipients receive their message directly
                    // List<String> recipients = (List<String>)map.get("recipients");
                    // if (recipients != null && !recipients.isEmpty() &&
                    // !recipients.contains(getSenderId())) {
                    // //message not for me
                    // return;
                    // }

                    if (pausedTopics.contains(map.get(Msg.Fields.topic.name()))) {
                        // log.info("Topic {} paused", map.get("name"));
                        // paused
                        return;
                    }
                    Msg doc = morphium.getMapper().deserialize(Msg.class, map);
                    if (doc.getSender().equals(getSenderId()))
                        return;

                    // Use atomic add operation - if already present, skip processing
                    if (!processingMessages.add(doc.getMsgId())) {
                        return;
                    }
                    if (doc.isExclusive()) {
                        // Check if already processed before attempting to lock
                        if (doc.getProcessedBy() != null && !doc.getProcessedBy().isEmpty()) {
                            processingMessages.remove(doc.getMsgId());
                            return;
                        }
                        if (!lockMessage(doc, getSenderId())) {
                            processingMessages.remove(doc.getMsgId());
                            return;
                        }
                    }
                    Msg current = morphium.reread(doc, getCollectionName(doc));
                    if (current == null) {
                        processingMessages.remove(doc.getMsgId());
                        return;
                    }

                    try {
                        if (current.getProcessedBy().contains(getSenderId())) {
                            // Don't unlock here - let lock timeout naturally
                            return;
                        }

                        if (l.markAsProcessedBeforeExec()) {
                            updateProcessedBy(current);
                        }
                        var ret = l.onMessage(this, current);
                        if (!running.get())
                            return;
                        if (ret == null && effectiveSettings.isAutoAnswer()) {
                            ret = new Msg(current.getTopic(), "received", "");
                        }
                        var deleted = false;
                        if (current.isDeleteAfterProcessing()) {
                            deleted = checkDeleteAfterProcessing(current);
                        }
                        if (!deleted && !l.markAsProcessedBeforeExec()) {
                            updateProcessedBy(current);
                        }
                        if (ret != null) {
                            // send answer
                            ret.setInAnswerTo(current.getMsgId());
                            ret.setRecipients(List.of(current.getSender()));
                            sendMessage(ret);
                        }
                    } catch (MessageRejectedException mre) {
                        unlock(current);
                        log.warn("Message rejected", mre);
                    } catch (Exception e) {
                        unlock(current);
                        log.error("Error processing message", e);
                    } finally {
                        processingMessages.remove(doc.getMsgId());
                    }
                } catch (Exception e) {
                    log.error("Error during change event processing", e);
                }
            };
            queueOrRun(r);
            return running.get();
        });
        ChangeStreamMonitor lockMonitor = new ChangeStreamMonitor(morphium, getLockCollectionName(n), false,
            effectiveSettings.getMessagingPollPause(), List.of(Doc.of("$match", Doc.of("operationType",
                Doc.of("$eq", "delete")))));
        lockMonitor.addListener((evt) -> {
            var id = evt.getId();
            if (morphium.createQueryFor(Msg.class).setCollectionName(getCollectionName(n)).f(Msg.Fields.msgId).eq(id)
                    .countAll() != 0) {
                pollTrigger.putIfAbsent(n, new AtomicInteger());
                pollTrigger.get(n).incrementAndGet();
            }

            return running.get();
        });
        monitorsByTopic.putIfAbsent(n, new ArrayList<>());
        monitorsByTopic.get(n).add(Map.of(MType.monitor, cm, MType.listener, l, MType.lockMonitor, lockMonitor));
        cm.start();
        pollAndProcess(n);
    }

    private void unlock(Msg msg) {
        if (msg.isExclusive()) {
            morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName(msg)).f("_id")
                    .eq(msg.getMsgId()).delete();
        } // no need to release lock, if message was not locked
    }

    @Override
    public void removeListenerForTopic(String topic, MessageListener l) {
        int idx = -1;

        for (var cm : monitorsByTopic.get(topic)) {
            idx++;

            if (cm.get(MType.listener) == l) {
                break;
            }
        }

        if (idx >= 0) {
            ((ChangeStreamMonitor) monitorsByTopic.get(topic).get(idx).get(MType.monitor)).terminate();
            ((ChangeStreamMonitor) monitorsByTopic.get(topic).get(idx).get(MType.lockMonitor)).terminate();
            monitorsByTopic.get(topic).remove(idx);
        }
        if (monitorsByTopic.get(topic).isEmpty()) {
            monitorsByTopic.remove(topic);
        }
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    @Override
    public MorphiumMessaging setSenderId(String id) {
        senderId = id;
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
    public void close() {
        terminate();
    }
    @Override
    public void terminate() {
        running.set(false);
        for (var e : monitorsByTopic.entrySet()) {
            for (var m : e.getValue()) {
                ((ChangeStreamMonitor) m.get(MType.monitor)).terminate();
                ((ChangeStreamMonitor) m.get(MType.lockMonitor)).terminate();
                ;
            }
        }
        if (directMessagesMonitor != null) directMessagesMonitor.terminate();
        if (threadPool != null) threadPool.shutdownNow();
        if (decouplePool != null) decouplePool.shutdownNow();
        if (monitorsByTopic != null) monitorsByTopic.clear();
        if (waitingForAnswers != null) waitingForAnswers.clear();
        if (waitingForCallbacks != null) waitingForCallbacks.clear();
        if (allMessagings != null) allMessagings.remove(this);
    }

    private void persistMessage(Msg m, boolean async) {
        m.setSenderHost(hostname);
        m.setSender(getSenderId());
        if (m.getRecipients() == null || m.getRecipients().isEmpty()) {
            if (async) {
                morphium.store(m, getCollectionName(m), aCallback);
            } else {
                morphium.store(m, getCollectionName(m), null);
            }

        } else {
            for (String rec : m.getRecipients()) {
                if (async) {
                    morphium.store(m, getDMCollectionName(rec), aCallback);
                } else {
                    morphium.store(m, getDMCollectionName(rec), null);
                }

            }
        }

    }

    @Override
    public void queueMessage(Msg m) {
        persistMessage(m, true);
    }

    @Override
    public void sendMessage(Msg m) {
        persistMessage(m, false);
    }

    @Override
    public long getNumberOfMessages() {
        long total = 0;
        // Sum pending per-topic messages this node could process
        try {
            for (var msgName : monitorsByTopic.keySet()) {
                Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName(msgName));
                // pending = not processed by anyone (exclusive) or not processed by me (broadcast)
                q1.f(Msg.Fields.sender).ne(getSenderId()).f("processed_by.0").notExists();
                total += q1.countAll();
            }

            // Include direct messages for this node in its DM collection
            Query<Msg> qdm = morphium.createQueryFor(Msg.class, getDMCollectionName());
            qdm.f(Msg.Fields.sender).ne(getSenderId()).f("processed_by.0").notExists();
            total += qdm.countAll();
        } catch (Exception e) {
            log.warn("Error calculating number of messages", e);
        }
        return total;
    }

    @Override
    public void sendMessageToSelf(Msg m) {

        m.setSender("self");
        m.setRecipient(getSenderId());
        morphium.store(m, getDMCollectionName(), null);
    }

    @Override
    public void queueMessagetoSelf(Msg m) {

        m.setSender("self");
        m.setRecipient(getSenderId());
        morphium.store(m, getDMCollectionName(), aCallback);
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
                throw new MessageTimeoutException("Did not receive answer for message " + theMessage.getTopic() + "/"
                                                  + requestMsgId + " in time (" + timeoutInMs + "ms)");
            }

            return firstAnswer;
        } catch (InterruptedException e) {
            log.error("Did not receive answer for message " + theMessage.getTopic() + "/" + requestMsgId
                      + " interrupted.", e);
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
    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout,
            boolean throwExceptionOnTimeout) {
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
                    throw new MessageTimeoutException("Did not receive any answer for message " + theMessage.getTopic()
                                                      + "/" + requestMsgId + "in time (" + timeout + ")");
                }

                if (System.currentTimeMillis() - start > timeout) {
                    break;
                }

                if (!running.get()) {
                    throw new SystemShutdownException("Messaging shutting down - abort waiting!");
                }

                LockSupport.parkNanos(
                                TimeUnit.MILLISECONDS.toNanos(morphium.getConfig().driverSettings().getIdleSleepTime()));
                // Thread.sleep(morphium.getConfig().driverSettings().getIdleSleepTime());
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
     * If sent message is exclusive, only one answer will be processed, otherwise
     * all incoming answers up to timeout
     * will be processed.
     *
     * @parameter theMessage to be sent
     * @parameter timoutInMs - milliseconds to wait until listener is removed
     * @parameter cb - the message callback
     */
    @Override
    public <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs, SingleCollectionMessaging.AsyncMessageCallback cb) {
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
            // create copy of settings, if same as morphiums
            effectiveSettings = m.getConfig().createCopy().messagingSettings();
        } else {
            effectiveSettings = overrides;
        }

        if (effectiveSettings.getSenderId() == null) {
            setSenderId(UUID.randomUUID().toString());
        } else {
            setSenderId(effectiveSettings.getSenderId());
        }
        threadPool = new ThreadPoolExecutor(
                        effectiveSettings.getThreadPoolMessagingCoreSize(),
                        effectiveSettings.getThreadPoolMessagingMaxSize(),
                        effectiveSettings.getThreadPoolMessagingKeepAliveTime(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        Thread.ofVirtual().name("msg-thr-", 0).factory());
    }

}
