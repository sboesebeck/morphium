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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.StatisticValue;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.query.Query;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:48
 * <p/>
 * Messaging implements a simple, threadsafe and messaging api. Used for cache
 * synchronization.
 * Msg can have several modes:
 * - LockedBy set to ALL (Exclusive Messages): every listener may process it (in
 * parallel), means 1->n. e.g. Cache sync
 * - LockedBy null (non exclusive messages): only one listener at a time
 * - Message listeners may return a Message as answer. Or throw a
 * MessageRejectedException.c
 */
@SuppressWarnings({"ConstantConditions", "unchecked", "UnusedDeclaration", "UnusedReturnValue", "BusyWait"})
public class Messaging extends Thread implements ShutdownListener {
    private static final Logger log = LoggerFactory.getLogger(Messaging.class);
    private final StatusInfoListener statusInfoListener;
    private String statusInfoListenerName = "morphium.status_info";
    private boolean statusInfoListenerEnabled = true;

    private final Morphium morphium;
    private boolean running;
    private int pause;

    private String id;
    private boolean autoAnswer = false;
    private String hostname;
    private boolean processMultiple;
    private ReceiveAnswers receiveAnswers = ReceiveAnswers.ONLY_MINE;

    private final List<MessageListener> listeners;

    private final Map<String, Long> pauseMessages = new ConcurrentHashMap<>();
    private Map<String, List<MessageListener>> listenerByName;
    private String queueName;
    private String lockCollectionName = null;
    private String collectionName = null;

    private ThreadPoolExecutor threadPool;
    private final ScheduledThreadPoolExecutor decouplePool;

    private boolean multithreadded;
    private int windowSize;
    private boolean useChangeStream;
    private ChangeStreamMonitor changeStreamMonitor;

    private final Map<MorphiumId, List<Msg>> waitingForAnswers = new ConcurrentHashMap<>();
    private final Map<MorphiumId, Msg> waitingForMessages = new ConcurrentHashMap<>();

    private final List<MorphiumId> processing = new CopyOnWriteArrayList<>();

    private final AtomicInteger skipped = new AtomicInteger(0);

    /**
     * attaches to the default queue named "msg"
     *
     * @param m - morphium
     * @param pause - pause between checks
     * @param processMultiple - process multiple messages at once, if false, only
     *        ony by one
     */
    public Messaging(Morphium m, int pause, boolean processMultiple) {
        this(m, null, pause, processMultiple);
    }

    public Messaging(Morphium m) {
        this(m, null, 500, false, false, 100);
    }

    public Messaging(Morphium m, int pause, boolean processMultiple, boolean multithreadded, int windowSize) {
        this(m, null, pause, processMultiple, multithreadded, windowSize);
    }

    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple) {
        this(m, queueName, pause, processMultiple, false, m.getConfig().getMessagingWindowSize());
    }

    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize) {
        this(m, queueName, pause, processMultiple, multithreadded, windowSize, m.isReplicaSet());
    }

    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize, boolean useChangeStream) {
        this(m, queueName, pause, processMultiple, multithreadded, windowSize, useChangeStream, ReceiveAnswers.ONLY_MINE);
    }

    @SuppressWarnings("CommentedOutCode")
    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize, boolean useChangeStream, ReceiveAnswers recieveAnswers) {
        setWindowSize(windowSize);
        setUseChangeStream(useChangeStream);
        setReceiveAnswers(recieveAnswers);
        setQueueName(queueName);
        setPause(pause);
        setProcessMultiple(processMultiple);
        morphium = m;
        statusInfoListener = new StatusInfoListener();
        setMultithreadded(multithreadded);
        decouplePool = new ScheduledThreadPoolExecutor(1);
        // noinspection unused,unused
        decouplePool.setThreadFactory(new ThreadFactory() {
            private final AtomicInteger num = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread ret = new Thread(r, "decouple_thr_" + num);
                num.set(num.get() + 1);
                ret.setDaemon(true);
                return ret;
            }
        });
        morphium.addShutdownListener(this);
        running = true;
        id = UUID.randomUUID().toString();
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

        m.ensureIndicesFor(Msg.class, getCollectionName());
        m.ensureIndicesFor(MsgLock.class, getLockCollectionName());
        listeners = new CopyOnWriteArrayList<>();
        listenerByName = new HashMap<>();
    }

    public void enableStatusInfoListener() {
        setStatusInfoListenerEnabled(true);
    }

    public void disableStatusInfoListener() {
        setStatusInfoListenerEnabled(false);
    }

    public String getStatusInfoListenerName() {
        return statusInfoListenerName;
    }

    public void setStatusInfoListenerName(String statusInfoListenerName) {
        listenerByName.remove(this.statusInfoListenerName);
        this.statusInfoListenerName = statusInfoListenerName;
        listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
    }

    public boolean isStatusInfoListenerEnabled() {
        return statusInfoListenerEnabled;
    }

    public void setStatusInfoListenerEnabled(boolean statusInfoListenerEnabled) {
        this.statusInfoListenerEnabled = statusInfoListenerEnabled;

        if (statusInfoListenerEnabled && !listenerByName.containsKey(statusInfoListenerName)) {
            listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
        } else if (!statusInfoListenerEnabled) {
            listenerByName.remove(statusInfoListenerName);
        }
    }

    public Map<String, List<String>> getListenerNames() {
        Map<String, List<String>> ret = new HashMap<>();
        Map<String, List<MessageListener>> localCopy = new HashMap<>(listenerByName);

        for (Map.Entry<String, List<MessageListener>> e : localCopy.entrySet()) {
            List<String> classes = new ArrayList<>();

            for (MessageListener lst : e.getValue()) {
                classes.add(lst.getClass().getName());
            }

            ret.put(e.getKey(), classes);
        }

        return ret;
    }

    public List<String> getGlobalListeners() {
        List<MessageListener> localCopy = new ArrayList<>(listeners);
        List<String> ret = new ArrayList<>();

        for (MessageListener lst : localCopy) {
            ret.add(lst.getClass().getName());
        }

        return ret;
    }

    public Map<String, Long> getThreadPoolStats() {
        String prefix = "messaging.threadpool.";
        return UtilsMap.of(prefix + "largest_poolsize", Long.valueOf(threadPool.getLargestPoolSize())).add(prefix + "task_count", threadPool.getTaskCount())
               .add(prefix + "core_size", (long) threadPool.getCorePoolSize()).add(prefix + "maximum_pool_size", (long) threadPool.getMaximumPoolSize())
               .add(prefix + "pool_size", (long) threadPool.getPoolSize()).add(prefix + "active_count", (long) threadPool.getActiveCount())
               .add(prefix + "completed_task_count", threadPool.getCompletedTaskCount());
    }

    private void initThreadPool() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>() {
            @SuppressWarnings("CommentedOutCode")
            @Override
            public boolean offer(Runnable e) {
                /*
                 * Offer it to the queue if there is 0 items already queued, else
                 * return false so the TPE will add another thread. If we return false
                 * and max threads have been reached then the RejectedExecutionHandler
                 * will be called which will do the put into the queue.
                 */
                int poolSize = threadPool.getPoolSize();
                int maximumPoolSize = threadPool.getMaximumPoolSize();

                if (poolSize >= maximumPoolSize || poolSize > threadPool.getActiveCount()) {
                    return super.offer(e);
                } else {
                    return false;
                }
            }
        };
        threadPool = new ThreadPoolExecutor(morphium.getConfig().getThreadPoolMessagingCoreSize(), morphium.getConfig().getThreadPoolMessagingMaxSize(),
          morphium.getConfig().getThreadPoolMessagingKeepAliveTime(), TimeUnit.MILLISECONDS, queue);
        threadPool.setRejectedExecutionHandler((r, executor)->{
            try {
                /*
                 * This does the actual put into the queue. Once the max threads
                 * have been reached, the tasks will then queue up.
                 */
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        // noinspection unused,unused
        threadPool.setThreadFactory(new ThreadFactory() {
            private final AtomicInteger num = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread ret = new Thread(r, "messaging " + num);
                num.set(num.get() + 1);
                ret.setDaemon(true);
                return ret;
            }
        });
    }

    public long getPendingMessagesCount() {
        Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName());
        q1.f(Msg.Fields.sender).ne(id).f(Msg.Fields.processedBy).eq(null);
        return q1.countAll();
    }

    public void removeMessage(Msg m) {
        morphium.delete(m, getCollectionName());
    }

    @Override
    public void run() {
        setName("Msg " + id);

        if (statusInfoListenerEnabled) {
            listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
        }

        if (useChangeStream) {
            // log.debug("Before running the changestream monitor - check of already
            // existing messages");
            try {
                findAndProcessMessages(processMultiple);

                if (multithreadded) {
                    // wait for preprocessing to finish
                    while (threadPool != null && threadPool.getActiveCount() > 0) {
                        Thread.sleep(morphium.getConfig().getIdleSleepTime());
                    }
                }
            } catch (Exception e) {
                log.error("Error processing existing messages in queue", e);
            }

            // pipeline for reducing incoming traffic
            List<Map<String, Object>> pipeline = new ArrayList<>();
            Map<String, Object> match = new LinkedHashMap<>();
            Map<String, Object> in = new LinkedHashMap<>();
            // in.put("$in", Arrays.asList("insert", "delete", "update"));
            in.put("$in", Arrays.asList("insert", "update"));
            match.put("operationType", in);
            pipeline.add(UtilsMap.of("$match", match));
            changeStreamMonitor = new ChangeStreamMonitor(morphium, getCollectionName(), true, pause, pipeline);
            changeStreamMonitor.addListener(evt->{
                // log.debug("incoming message via changeStream");
                if (!running)
                    return false;
                try {
                    if (evt == null || evt.getOperationType() == null) {
                        return running;
                    }

                    if (evt.getOperationType().equals("insert")) {
                        // insert => new Message
                        Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());

                        if (obj.isExclusive() && obj.getProcessedBy().size() != 0) {
                            // inserted already processed message?!?!?
                            return running;
                        }

                        if (processing.contains(obj.getMsgId())) {
                            skipped.incrementAndGet();
                            return running;
                        }

                        processing.add(obj.getMsgId());

                        if (obj.getRecipients() != null && !obj.getRecipients().contains(getSenderId())) {
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (obj.getSender().equals(id)) {
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (obj.getInAnswerTo() != null) {
                            handleAnswer(obj);
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (listenerByName.get(obj.getName()) == null && listeners.size() == 0) {
                            // ignoring incoming message, we do not have listener for
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (pauseMessages.containsKey(obj.getName())) {
                            skipped.incrementAndGet();
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (obj.getProcessedBy().contains(id)) {
                            // already processed it
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (obj.getSender().equals(id) || obj.getProcessedBy().contains(id) || (obj.getRecipients() != null && !obj.getRecipients().contains(id))) {
                            // ignoring my own messages
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        try {
                            if (obj.isExclusive() && (obj.getRecipients() == null || obj.getRecipients().contains(id)) && obj.getProcessedBy().size() == 0) {
                                // log.info(id + ": Exclusive message inserted - " + obj.getMsgId());
                                lockAndProcess(obj);
                            } else if (!obj.isExclusive() || (obj.getRecipients() != null && obj.getRecipients().contains(id))) {
                                // I need process this new message... it is either for all or for me directly
                                processMessage(obj);
                            }
                        } catch (Exception e) {
                            log.error("Error during message processing ", e);
                        }

                        // processing.remove(obj.getMsgId());
                        // } else if (evt.getOperationType().equals("delete")) {
                        // skipped.incrementAndGet();
                    } else if (evt.getOperationType().equals("update")) {
                        Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());

                        if (obj == null) {
                            return running;
                        }

                        if (obj.isExclusive() && obj.getProcessedBy().size() > 0) {
                            return running;
                        }

                        if (obj.getProcessedBy().contains(id)) {
                            return running;
                        }

                        if (processing.contains(obj.getMsgId())) {
                            // already processing it
                            return running;
                        }

                        processing.add(obj.getMsgId());

                        if (obj.getSender().equals(id) || (obj.getRecipients() != null && !obj.getRecipients().contains(id))) {
                            // ignoring my own messages
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (obj.getInAnswerTo() != null) {
                            handleAnswer(obj);
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (listenerByName.get(obj.getName()) == null && listeners.size() == 0) {
                            // ignoring incoming message, we do not have listener for
                            processing.remove(obj.getMsgId());
                            return running;
                        }

                        if (pauseMessages.containsKey(obj.getName())) {
                            processing.remove(obj.getMsgId());
                            return running;
                        }


                        if (obj.isExclusive()) {
                            // locking
                            // log.info("Exclusive message update: " + obj.getMsgId());
                            lockAndProcess(obj);
                        } else {
                            processMessage(obj);
                        }

                        // processing.remove(obj.getMsgId());
                    }
                } catch (Exception e) {
                    log.error("Error during event processing in changestream", e);
                }
                return running;
            });
            changeStreamMonitor.start();
        }

        skipped.incrementAndGet();

        // always run this find in addition to changestream
        while (running) {
            try {
                if (skipped.get() > 0 || !useChangeStream) {
                    morphium.inc(StatisticKeys.PULL);
                    StatisticValue sk = morphium.getStats().get(StatisticKeys.PULLSKIP);
                    sk.set(sk.get() + skipped.get());
                    skipped.set(0);
                    findAndProcessMessages(processMultiple);
                } else {
                    morphium.inc(StatisticKeys.SKIPPED_MSG_UPDATES);
                }
            } catch (Throwable e) {
                if (running) {
                    log.error("Unhandled exception " + e.getMessage(), e);
                } else {
                    break;
                }
            } finally {
                try {
                    sleep(pause);
                } catch (InterruptedException ignored) {
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Messaging " + id + " stopped!");
        }

        if (!running) {
            listeners.clear();
            listenerByName.clear();
        }
    }

    @SuppressWarnings("CommentedOutCode")
    private synchronized void handleAnswer(Msg obj) {
        if (waitingForMessages.containsKey(obj.getInAnswerTo())) {
            // we're expecting this message!
            updateProcessedBy(obj);

            if (waitingForAnswers.containsKey(obj.getInAnswerTo()) && !waitingForAnswers.get(obj.getInAnswerTo()).contains(obj)) {
                waitingForAnswers.get(obj.getInAnswerTo()).add(obj);
            }
        }

        if (!receiveAnswers.equals(ReceiveAnswers.NONE)) {
            if (receiveAnswers.equals(ReceiveAnswers.ALL) || (obj.getRecipients() != null && obj.getRecipients().contains(id))) {
                try {
                    if (obj.isExclusive() && obj.getProcessedBy().size() > 0) {
                        // exclusive message already processed
                        return;
                    }

                    if (obj.isExclusive()) {
                        lockAndProcess(obj);
                    } else {
                        // if (obj.getName().equals(statusInfoListenerName)) {
                        // log.info(id + ":: processing answer from statusInfoListener");
                        // }
                        processMessage(obj);
                    }
                } catch (Exception e) {
                    log.error("Error during message processing ", e);
                }
            }
        }
    }

    /**
     * pause processing for certain messages
     *
     * @param name
     */

    public void pauseProcessingOfMessagesNamed(String name) {
        // log.debug("PAusing processing for "+name);
        pauseMessages.putIfAbsent(name, System.currentTimeMillis());
    }

    public List<String> getPausedMessageNames() {
        return new ArrayList<>(pauseMessages.keySet());
    }

    /**
     * unpause processing
     *
     * @param name
     * @return duration or null
     */
    @SuppressWarnings("CommentedOutCode")
    public Long unpauseProcessingOfMessagesNamed(String name) {
        // log.debug("unpausing processing for "+name);
        if (!pauseMessages.containsKey(name)) {
            return 0L;
        }

        Long ret = pauseMessages.remove(name);

        if (ret != null) {
            ret = System.currentTimeMillis() - ret;
        }

        skipped.incrementAndGet();
        return ret;
    }

    // public void findAndProcessPendingMessages(String name) {
    // Runnable r = ()->{
    // while (true) {
    // List<MorphiumId> messages = lockAndGetMessages(name, processMultiple);
    //
    // if (messages == null || messages.size() == 0) {
    // break;
    // }
    //
    // processMessages(messages);
    //
    // try {
    // Thread.sleep(pause);
    // } catch (InterruptedException e) {
    // // Swallow
    // }
    // }
    // };
    // queueOrRun(r);
    // }

    private List<MorphiumId> getMessagesForProcessing(boolean multiple) {
        if (!running) {
            return new ArrayList<>();
        }

        Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName());

        if (listenerByName.isEmpty() && listeners.isEmpty()) {
            if (getReceiveAnswers().equals(ReceiveAnswers.ALL)) {
                return q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.inAnswerTo).ne(null).limit(windowSize).idList();
            } else if (getReceiveAnswers().equals(ReceiveAnswers.ONLY_MINE)) {
                // No listeners - only answers will be processed
                return q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.inAnswerTo).in(waitingForMessages.keySet()).limit(windowSize).idList();
            } else { // NO_ANSWER_PROCESSING
                return new ArrayList<>();
            }
        }

        // locking messages.. and getting broadcasts
        var preLockedIds = morphium.createQueryFor(MsgLock.class).setCollectionName(getCollectionName() + "_lck").f(MsgLock.Fields.lockId).ne(id).idList();
        preLockedIds.addAll(new ArrayList<>(processing));
        // q1: Exclusive messages, not locked yet, not processed yet
        var q1 = q.q().f("_id").nin(preLockedIds).f(Msg.Fields.sender).ne(id).f(Msg.Fields.recipients).in(Arrays.asList(null, id)).f(Msg.Fields.exclusive).eq(true).f("processed_by.0").eq(null);
        // q2: non-exclusive messages, cannot be locked, not processed by me yet
        var q2 = q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.recipients).in(Arrays.asList(null, id)).f(Msg.Fields.exclusive).ne(true).f(Msg.Fields.processedBy).ne(id);
        q.or(q1, q2);
        Set<String> pausedMessagesKeys = pauseMessages.keySet();

        // not searching for paused messages
        if (!pauseMessages.isEmpty()) {
            q.f(Msg.Fields.name).nin(pausedMessagesKeys);
        }

        // Only handle messages we have listener for
        if (listeners.isEmpty() && !listenerByName.isEmpty()) {
            q.f(Msg.Fields.name).in(listenerByName.keySet());
        }

        q.setLimit(windowSize);
        q.sort(Msg.Fields.priority, Msg.Fields.timestamp);
        List<MorphiumId> lockedIds = new ArrayList<>();
        // just trigger unprocessed messages for Changestream...
        FindCommand fnd = null;

        try {
            int ws = windowSize;

            if (!multiple) {
                ws = 1;
            }

            // get IDs of messages to process
            fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(Msg.class)));
            fnd.setDb(morphium.getDatabase());
            fnd.setFilter(q.toQueryObject());
            fnd.setProjection(Doc.of("_id", 1, "ttl", 1, "timing_out", 1, "exclusive", 1, "processed_by", 1));
            fnd.setLimit(ws);
            fnd.setBatchSize(q.getBatchSize());
            fnd.setSort(q.getSort());
            fnd.setSkip(q.getSkip());
            fnd.setColl(getCollectionName());
            var result = fnd.execute();

            if (!result.isEmpty()) {
                for (Map<String, Object> el : result) {
                    if (el.get("exclusive") == null || el.get("exclusive").equals(Boolean.FALSE)) {
                        // no lock necessary
                        if (!processing.contains(el.get("_id"))) {
                            lockedIds.add((MorphiumId) el.get("_id"));
                        }
                    } else {
                        if (el.get("processed_by") != null && ((List) el.get("processed_by")).size() > 0) {
                            // skipping already processed
                            log.error("Should not get a processed exclusive message");
                            continue;
                        }

                        MsgLock l = morphium.findById(MsgLock.class, el.get("_id"), getLockCollectionName());

                        if (l != null && l.getLockId().equals(id)) {
                            lockedIds.add((MorphiumId) el.get("_id"));
                        } else {
                            l = new MsgLock((MorphiumId) el.get("_id"));
                            l.setLockId(id);

                            if (el.containsKey("timing_out") && el.get("timing_out").equals(Boolean.TRUE)) {
                                var ttl = (Long) el.get("ttl");
                                l.setDeleteAt(new Date(System.currentTimeMillis() + ttl));
                            }

                            try {
                                morphium.insert(l, getCollectionName() + "_lck", null);
                                lockedIds.add(l.getId());
                            } catch (Exception e) {
                                // could not lock!
                            }
                        }
                    }
                }
            }

            if (q.countAll() > lockedIds.size()) {
                skipped.incrementAndGet();
            }

            return lockedIds;
        } catch (Exception e) {
            log.error("Error while processing", e);
            return null;
        } finally {
            if (fnd != null) {
                fnd.releaseConnection();
            }
        }
    }

    public void triggerCheck() {
        skipped.incrementAndGet();
    }

    private void findAndProcessMessages(boolean multiple) {
        if (!running) {
            return;
        }

        List<MorphiumId> messages = getMessagesForProcessing(multiple);

        if (messages == null) {
            return;
        }

        if (messages.size() == 0) {
            return;
        }

        processMessages(messages);
    }

    @SuppressWarnings("CommentedOutCode")
    private void lockAndProcess(Msg obj) {
        if (lockMessage(obj, id)) {
            processMessage(obj);
        } else {
            // not locked
            skipped.incrementAndGet();
            processing.remove(obj.getMsgId());
            return;
        }
    }

    public MsgLock getLock(Msg m) {
        return morphium.findById(MsgLock.class, m.getMsgId(), getLockCollectionName());
    }

    public String getLockCollectionName() {
        if (lockCollectionName == null) {
            lockCollectionName = getCollectionName() + "_lck";
        }

        return lockCollectionName;
    }

    public boolean lockMessage(Msg m, String lockId) {
        return lockMessage(m, lockId, null);
    }

    public boolean releaseLock(Msg m) {
        var ret = morphium.createQueryFor(MsgLock.class).setCollectionName(getCollectionName() + "_lck").f("_id").eq(m.getMsgId()).delete();
        return (ret.containsKey("n") && ret.get("n").equals(Integer.valueOf(1)));
    }

    public boolean lockMessage(Msg m, String lockId, Date delAt) {
        MsgLock lck = new MsgLock(m);
        lck.setLockId(lockId);

        if (delAt != null) {
            lck.setDeleteAt(delAt);
        }

        try {
            morphium.insert(lck, getCollectionName() + "_lck", null);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private synchronized void processMessage(Msg ms) {
        var msg = morphium.reread(ms, getCollectionName());

        // Not locked by me
        if (msg == null) {
            // if (log.isDebugEnabled()) {
            // log.debug("Message was deleted before processing could happen!");
            // }
            return;
        }

        // I am the sender?
        if (msg.getSender().equals(getSenderId())) {
            log.error("This should have been filtered out before alreaday!!!");
            unlockIfExclusive(msg);
            removeProcessingFor(msg);
            return;
        }

        // outdated message
        if (msg.isTimingOut() && msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
            // Delete outdated msg!
            // if (log.isDebugEnabled()) {
            // log.debug(getSenderId() + ": Found outdated message - deleting it!");
            // }
            morphium.delete(msg, getCollectionName());
            processing.remove(msg.getMsgId());
            unlockIfExclusive(msg);
            return;
        }

        if (msg.isExclusive() && msg.getProcessedBy().size() > 0) {
            // exclusive message already processed!
            removeProcessingFor(msg);
            // remove lock
            unlockIfExclusive(msg);
            return;
        }

        if (msg.getProcessedBy().contains(id)) {
            unlockIfExclusive(msg);
            removeProcessingFor(msg);
            return;
        }

        // if (msg.isExclusive()){
        // var
        // cnt=morphium.createQueryFor(MsgLock.class).setCollectionName(getCollectionName()
        // + "_lck").f("_id").eq(msg.getMsgId()).countAll();
        // if (cnt!=1){
        // log.error("Unlocked exclusive message processing!");
        // }
        // }
        //
        if (msg.getInAnswerTo() != null) {
            if (waitingForMessages.containsKey(msg.getInAnswerTo())) {
                updateProcessedBy(msg);
                List<Msg> lst = waitingForAnswers.get(msg.getInAnswerTo());

                if (lst != null && !lst.contains(msg)) {
                    waitingForAnswers.get(msg.getInAnswerTo()).add(msg);
                }
            }

            if (receiveAnswers.equals(ReceiveAnswers.NONE) || (receiveAnswers.equals(ReceiveAnswers.ONLY_MINE) && msg.getRecipients() != null && !msg.getRecipients().contains(id))) {
                unlockIfExclusive(msg);
                removeProcessingFor(msg);
                return;
            }
        }

        if (listeners.isEmpty() && listenerByName.isEmpty()) {
            // message cannot be processed, as no listener is defined and message is no
            // answer.
            if (log.isDebugEnabled()) {
                log.debug("Not further processing - no listener for non answer message");
            }

            updateProcessedBy(msg);
            removeProcessingFor(msg);
            unlockIfExclusive(msg);
            return;
        }

        Runnable r = ()->{
            boolean wasProcessed = false;
            boolean wasRejected = false;
            List<MessageRejectedException> rejections = new ArrayList<>();
            List<MessageListener> lst = new ArrayList<>(listeners);

            if (listenerByName.get(msg.getName()) != null) {
                lst.addAll(listenerByName.get(msg.getName()));
            }

            if (lst.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug(getSenderId() + ": Message did not have a listener registered: " + msg.getName());
                }

                wasProcessed = true;
            }

            for (MessageListener l : lst) {
                try {
                    if (pauseMessages.containsKey(msg.getName())) {
                        // paused - do not process
                        // log.warn("Received paused message?!?!? "+msg1.getMsgId());
                        processing.remove(msg.getMsgId());
                        wasProcessed = false;
                        skipped.incrementAndGet();
                        break;
                    }

                    if (l.markAsProcessedBeforeExec()) {
                        updateProcessedBy(msg);
                    }

                    Msg answer = l.onMessage(Messaging.this, msg);
                    wasProcessed = true;

                    if (autoAnswer && answer == null) {
                        answer = new Msg(msg.getName(), "received", "");
                    }

                    if (answer != null) {
                        msg.sendAnswer(Messaging.this, answer);

                        if (answer.getRecipients() == null) {
                            log.warn("Recipient of answer is null?!?!");
                        }
                    }

                    if (msg.isDeleteAfterProcessing()) {
                        if (msg.getDeleteAfterProcessingTime() == 0) {
                            morphium.delete(msg, getCollectionName());
                        } else {
                            msg.setDeleteAt(new Date(System.currentTimeMillis() + msg.getDeleteAfterProcessingTime()));
                            morphium.set(msg, getCollectionName(), Msg.Fields.deleteAt, msg.getDeleteAt());

                            if (!l.markAsProcessedBeforeExec()) {
                                morphium.addToSet(morphium.createQueryFor(Msg.class).setCollectionName(getCollectionName()).f("_id").eq(msg.getMsgId()), "processed_by", id);
                            }
                        }
                    }
                } catch (MessageRejectedException mre) {
                    // if (log.isDebugEnabled()){
                    // log.info("Message was rejected by listener", mre);
                    // } else {
                    log.info(id + ": Message was rejected by listener: " + mre.getMessage());
                    // }
                    wasRejected = true;
                    rejections.add(mre);
                    skipped.incrementAndGet();
                } catch (Exception e) {
                    log.error(id + ": listener Processing failed", e);

                    if (msg.isDeleteAfterProcessing()) {
                        if (msg.getDeleteAfterProcessingTime() == 0) {
                            morphium.delete(msg, getCollectionName());
                            unlockIfExclusive(msg);
                        } else {
                            msg.setDeleteAt(new Date(System.currentTimeMillis() + msg.getDeleteAfterProcessingTime()));
                            morphium.set(msg, getCollectionName(), Msg.Fields.deleteAt, msg.getDeleteAt());
                            if (msg.isExclusive()){
                                morphium.createQueryFor(MsgLock.class,getLockCollectionName()).f("_id").eq(msg.getMsgId()).set(MsgLock.Fields.deleteAt,msg.getDeleteAt());
                            }
                        }
                    }
                }
            }

            // }

            if (wasRejected) {
                for (MessageRejectedException mre : rejections) {
                    if (mre.getRejectionHandler() != null) {
                        try {
                            mre.getRejectionHandler().handleRejection(this, msg);
                        } catch (Exception e) {
                            log.error("Error in rejection handling", e);
                        }
                    } else {
                        if (mre.isSendAnswer()) {
                            Msg answer = new Msg(msg.getName(), "message rejected by listener", mre.getMessage());
                            msg.sendAnswer(Messaging.this, answer);
                        }

                        if (mre.isContinueProcessing()) {
                            UpdateMongoCommand cmd = null;

                            try {
                                cmd = new UpdateMongoCommand(morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
                                cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
                                cmd.addUpdate(Doc.of("_id", msg.getMsgId()), Doc.of("$addToSet", Doc.of("processed_by", id)), null, false, false, null, null, null);
                                cmd.execute();
                                unlockIfExclusive(msg);
                            } catch (MorphiumDriverException e) {
                                log.error("Error unlocking message", e);
                            } finally {
                                if (cmd != null) {
                                    cmd.getConnection().release();
                                }
                            }

                            log.debug(id + ": Message will be re-processed by others");
                        }
                    }
                }
            } else if (!wasProcessed && !wasRejected) { // keeping !wasRejected to make it more clear
                if (!pauseMessages.containsKey(msg.getName())) {
                    log.error("message was not processed - an error occured");
                }
            } else if (wasProcessed && !msg.getProcessedBy().contains(id)) {
                updateProcessedBy(msg);
            }

            removeProcessingFor(msg);

            unlockIfExclusive(msg);
        };
        queueOrRun(r);
    }

    private void unlockIfExclusive(Msg msg) {
        if (msg.isExclusive()) {
            // remove _own_ lock
            morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName()).f("_id").eq(msg.getMsgId()).f("lock_id").eq(id).remove();
        }
    }

    private void processMessages(List<MorphiumId> messages) {
        for (final MorphiumId mId : messages) {
            if (!running) {
                return;
            }

            processing.add(mId);

            try {
                FindCommand cmd = new FindCommand(morphium.getDriver().getPrimaryConnection(null));
                cmd.setFilter(Doc.of("_id", mId));
                cmd.setLimit(1);
                cmd.setBatchSize(1);
                cmd.setColl(getCollectionName());
                cmd.setDb(morphium.getDatabase());
                var result = cmd.execute();
                cmd.releaseConnection();

                if (result.isEmpty()) {
                    processing.remove(mId);
                    continue;
                }

                final Msg msg = morphium.getMapper().deserialize(Msg.class, result.get(0));

                if (msg == null) {
                    continue;
                }

                processMessage(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void removeProcessingFor(Msg msg) {
        Runnable rb = new RemoveProcessTask(processing, msg.getMsgId());
        long timeout = msg.getTtl();

        if (msg.getTtl() == 0 || !msg.isTimingOut()) {
            timeout = 1000;
        }

        while (true) {
            try {
                if (!decouplePool.isTerminated() && !decouplePool.isTerminating() && !decouplePool.isShutdown()) {
                    decouplePool.schedule(rb, timeout, TimeUnit.MILLISECONDS); // avoid re-executing message
                }

                break;
            } catch (RejectedExecutionException ex) {
                try {
                    Thread.sleep(pause);
                } catch (InterruptedException e) {
                    // Swallow
                }
            }
        }
    }

    private void updateProcessedBy(Msg msg) {
        if (msg == null) {
            return;
        }

        // if (msg.getDeleteAfterProcessingTime() == 0 && msg.isDeleteAfterProcessing())
        // {
        // morphium.remove(msg);// Delete it right now!
        // return;
        // }

        if (msg.getProcessedBy().contains(id)) {
            return;
        }

        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName());
        idq.f(Msg.Fields.msgId).eq(msg.getMsgId());
        msg.getProcessedBy().add(id);
        Map<String, Object> qobj = idq.toQueryObject();
        Map<String, Object> set = Doc.of("processed_by", id);
        Map<String, Object> update = Doc.of("$addToSet", set);
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
            cmd.addUpdate(qobj, update, null, false, false, null, null, null);
            Map<String, Object> ret = cmd.execute();

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

            msg.getProcessedBy().add(id);
        } catch (MorphiumDriverException e) {
            log.error("Error updating processed by - this might lead to duplicate execution!", e);
        } finally {
            if (cmd != null) {
                cmd.getConnection().release();
            }
        }
    }

    private void queueOrRun(Runnable r) {
        if (multithreadded) {
            boolean queued = false;

            while (!queued) {
                try {
                    // throtteling to windowSize - do not create more threads than windowSize
                    while (threadPool.getActiveCount() > windowSize) {
                        Thread.sleep(morphium.getConfig().getIdleSleepTime());
                    }

                    threadPool.execute(r);
                    queued = true;
                } catch (Throwable ignored) {
                }
            }
        } else {
            r.run();
        }
    }

    public String getCollectionName() {
        if (collectionName == null) {
            if (queueName == null || queueName.isEmpty()) {
                collectionName = "msg";
            } else {
                collectionName = "mmsg_" + queueName;
            }
        }

        return collectionName;
    }

    public void addListenerForMessageNamed(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            HashMap<String, List<MessageListener>> c = (HashMap)((HashMap) listenerByName).clone();
            c.put(n, new ArrayList<>());
            listenerByName = c;
        }

        if (listenerByName.get(n).contains(l)) {
            log.error("cowardly refusing to add already registered listener for name " + n);
        } else {
            listenerByName.get(n).add(l);
        }

        skipped.incrementAndGet();
    }

    public void removeListenerForMessageNamed(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            return;
        }

        HashMap<String, List<MessageListener>> c = (HashMap)((HashMap) listenerByName).clone();
        c.get(n).remove(l);

        if (c.get(n).isEmpty()) {
            c.remove(n);
        }

        listenerByName = c;
    }

    public String getSenderId() {
        return id;
    }

    public Messaging setSenderId(String id) {
        this.id = id;
        return this;
    }

    public int getPause() {
        return pause;
    }

    public Messaging setPause(int pause) {
        this.pause = pause;
        return this;
    }

    public boolean isRunning() {
        if (useChangeStream) {
            return changeStreamMonitor != null && changeStreamMonitor.isRunning();
        }

        return running;
    }

    public void terminate() {
        running = false;
        listenerByName.clear();
        listeners.clear();
        waitingForMessages.clear();
        waitingForMessages.clear();
        processing.clear();
        skipped.set(0);

        if (decouplePool != null) {
            try {
                int sz = decouplePool.shutdownNow().size();

                if (log.isDebugEnabled()) {
                    log.debug("Shutting down with " + sz + " runnables still scheduled");
                }
            } catch (Exception e) {
                log.warn("Exception when shutting down decouple-pool", e);
            }
        }

        morphium.removeShutdownListener(this);

        if (threadPool != null) {
            try {
                int sz = threadPool.shutdownNow().size();

                if (log.isDebugEnabled()) {
                    log.debug("Shutting down with " + sz + " runnables still pending in pool");
                }
            } catch (Exception e) {
                log.warn("Exception when shutting down threadpool");
            }
        }

        if (changeStreamMonitor != null) {
            changeStreamMonitor.terminate();
        }

        if (isAlive()) {
            try {
                interrupt();
            } catch (Exception e) {
                log.warn("Exception when interrupint messaging thread", e);
            }
        }

        int retry = 0;

        while (isAlive()) {
            try {
                sleep(150);
            } catch (InterruptedException e) {
                // swallow
            }

            retry++;

            if (retry > morphium.getConfig().getMaxWaitTime() / 150) {
                log.warn("Force stopping messaging!");
                // noinspection deprecation
                stop();
            }

            if (retry > 2 * morphium.getConfig().getMaxWaitTime() / 150) {
                throw new RuntimeException("Could not terminate Messaging!");
            }
        }
    }

    public void addMessageListener(MessageListener l) {
        if (listeners.contains(l)) {
            log.error("Cowardly refusing to add already registered listener");
        } else {
            listeners.add(l);
        }

        skipped.incrementAndGet();
    }

    public void removeMessageListener(MessageListener l) {
        listeners.remove(l);
    }

    public void queueMessage(final Msg m) {
        storeMsg(m, true);
    }

    @Override

    public synchronized void start() {
        super.start();
        // if (useChangeStream) {
        // try {
        // Thread.sleep(250);
        // //wait for changestream to kick in ;-)
        // } catch (Exception e) {
        // log.error("error:" + e.getMessage());
        // }
        // }
    }

    public void sendMessage(Msg m) {
        storeMsg(m, false);
    }

    public long getNumberOfMessages() {
        return getPendingMessagesCount();
    }

    private void storeMsg(Msg m, boolean async) {
        AsyncOperationCallback cb = null;

        if (async) {
            // noinspection unused,unused
            cb = new AsyncOperationCallback() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query q, long duration, List result, Object entity, Object... param) {
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query q, long duration, String error, Throwable t, Object entity, Object... param) {
                    log.error("Error storing msg", t);
                }
            };
        }

        m.setSender(id);
        m.setSenderHost(hostname);
        morphium.insert(m, getCollectionName(), cb);
    }

    public void sendMessageToSelf(Msg m) {
        sendMessageToSelf(m, false);
    }

    public void queueMessagetoSelf(Msg m) {
        sendMessageToSelf(m, true);
    }

    private void sendMessageToSelf(Msg m, boolean async) {
        AsyncOperationCallback cb = null;

        // noinspection StatementWithEmptyBody
        if (async) {
            // noinspection unused,unused
            cb = new AsyncCallbackAdapter<>();
        }

        m.setSender("self");
        m.addRecipient(id);
        m.setSenderHost(hostname);
        morphium.insert(m, getCollectionName(), cb);
    }

    public boolean isAutoAnswer() {
        return autoAnswer;
    }

    public Messaging setAutoAnswer(boolean autoAnswer) {
        this.autoAnswer = autoAnswer;
        return this;
    }

    @Override
    public void onShutdown(Morphium m) {
        try {
            running = false;

            if (threadPool != null) {
                threadPool.shutdown();
                Thread.sleep(200);

                if (threadPool != null) {
                    threadPool.shutdownNow();
                }

                threadPool = null;
            }

            if (changeStreamMonitor != null) {
                changeStreamMonitor.terminate();
            }
        } catch (Exception e) {
            e.printStackTrace();
            // swallow
        }
    }

    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs) {
        return sendAndAwaitFirstAnswer(theMessage, timeoutInMs, true);
    }

    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs, boolean throwExceptionOnTimeout) {
        theMessage.setMsgId(new MorphiumId());
        waitingForAnswers.put(theMessage.getMsgId(), new ArrayList<>());
        waitingForMessages.put(theMessage.getMsgId(), theMessage);
        sendMessage(theMessage);
        long start = System.currentTimeMillis();

        while (waitingForAnswers.get(theMessage.getMsgId()).size() == 0) {
            if (!running) {
                throw new SystemShutdownException("Messaging shutting down - abort waiting!");
            }

            if (System.currentTimeMillis() - start > timeoutInMs) {
                log.error("Did not receive answer " + theMessage.getName() + "/" + theMessage.getMsgId() + " in time (" + timeoutInMs + "ms)");
                waitingForMessages.remove(theMessage.getMsgId());

                if (throwExceptionOnTimeout) {
                    throw new MessageTimeoutException("Did not receive answer for message " + theMessage.getName() + "/" + theMessage.getMsgId() + " in time (" + timeoutInMs + "ms)");
                }

                return null;
            }

            try {
                Thread.sleep(morphium.getConfig().getIdleSleepTime());
            } catch (InterruptedException e) {
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("got message after: " + (System.currentTimeMillis() - start) + "ms");
        }

        waitingForMessages.remove(theMessage.getMsgId());
        return (T) waitingForAnswers.remove(theMessage.getMsgId()).get(0);
    }

    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout) {
        return sendAndAwaitAnswers(theMessage, numberOfAnswers, timeout, false);
    }

    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout, boolean throwExceptionOnTimeout) {
        if (theMessage.getMsgId() == null) {
            theMessage.setMsgId(new MorphiumId());
        }

        waitingForAnswers.put(theMessage.getMsgId(), new ArrayList<>());
        waitingForMessages.put(theMessage.getMsgId(), theMessage);
        sendMessage(theMessage);
        long start = System.currentTimeMillis();

        while (running) {
            if (waitingForAnswers.get(theMessage.getMsgId()).size() > 0) {
                if (numberOfAnswers > 0 && waitingForAnswers.get(theMessage.getMsgId()).size() >= numberOfAnswers) {
                    break;
                }

                // time up - return all answers that were received
            }

            // Did not receive any message in time
            if (throwExceptionOnTimeout && System.currentTimeMillis() - start > timeout && (waitingForAnswers.get(theMessage.getMsgId()).isEmpty())) {
                throw new MessageTimeoutException("Did not receive any answer for message " + theMessage.getName() + "/" + theMessage.getMsgId() + "in time (" + timeout + ")");
            }

            if (System.currentTimeMillis() - start > timeout) {
                break;
            }

            if (!running) {
                throw new SystemShutdownException("Messaging shutting down - abort waiting!");
            }

            try {
                Thread.sleep(morphium.getConfig().getIdleSleepTime());
            } catch (InterruptedException e) {
                // ignore
            }
        }

        waitingForMessages.remove(theMessage.getMsgId());
        return (List<T>) waitingForAnswers.remove(theMessage.getMsgId());
    }

    public boolean isProcessMultiple() {
        return processMultiple;
    }

    public Messaging setProcessMultiple(boolean processMultiple) {
        this.processMultiple = processMultiple;
        return this;
    }

    public String getQueueName() {
        return queueName;
    }

    public Messaging setQueueName(String queueName) {
        this.queueName = queueName;
        collectionName=null;
        lockCollectionName=null;
        return this;
    }

    public boolean isMultithreadded() {
        return multithreadded;
    }

    public Messaging setMultithreadded(boolean multithreadded) {
        if (!multithreadded && threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        } else if (multithreadded && threadPool == null) {
            initThreadPool();
        }

        this.multithreadded = multithreadded;
        return this;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public Messaging setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    /**
     * Receive answers=false, onMessage is not called, when answers come in
     * if true, onMessage is called for all answers
     * this is not affecting the sendAndWaitFor-Methods!
     *
     * @return
     */
    public boolean isReceiveAnswers() {
        return !receiveAnswers.equals(ReceiveAnswers.NONE);
    }

    public ReceiveAnswers getReceiveAnswers() {
        return receiveAnswers;
    }

    /**
     * Receive answers=false, onMessage is not called, when answers come in
     * if true, onMessage is called for all answers
     * this is not affecting the sendAndWaitFor-Methods!
     */
    public void setReceiveAnswers(ReceiveAnswers receiveAnswers) {
        this.receiveAnswers = receiveAnswers;
    }

    public boolean isUseChangeStream() {
        return useChangeStream;
    }

    public int getRunningTasks() {
        if (threadPool != null) {
            return threadPool.getActiveCount();
        }

        return 0;
    }

    Morphium getMorphium() {
        return morphium;
    }

    /**
     * Just for the understanding: if you do not use the changestream, messaging
     * will
     * poll for new mesages regularly (configured by the pause)
     * setPolling(true) == setUseChangeStream(false)!!!!!!
     **/
    public Messaging setPolling(boolean doPolling) {
        useChangeStream = !doPolling;
        return this;
    }

    public Messaging setUseChangeStream(boolean useChangeStream) {
        this.useChangeStream = useChangeStream;
        return this;
    }

    public enum ReceiveAnswers {
        NONE, ONLY_MINE, ALL,
    }
    public static class MessageTimeoutException extends RuntimeException {
        public MessageTimeoutException(String msg) {
            super(msg);
        }
    }
    public static class SystemShutdownException extends RuntimeException {
        public SystemShutdownException(String msg) {
            super(msg);
        }
    }
}
