package de.caluga.morphium.messaging;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.Messaging;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.query.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
@Messaging(name = "StandardMessaging", description = "Standard message queueing implementation")
public class StdMessaging extends Thread implements ShutdownListener, MorphiumMessaging {
    private static final Logger log = LoggerFactory.getLogger(StdMessaging.class);
    private final StatusInfoListener statusInfoListener = new StatusInfoListener();
    private String statusInfoListenerName = "morphium.status_info";
    private boolean statusInfoListenerEnabled = true;
    private Morphium morphium;
    private boolean running = true;
    private int pause = 100;

    private String id;
    private boolean autoAnswer = false;
    private String hostname;

    private final List<MessageListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Long> pauseMessages = new ConcurrentHashMap<>();
    private Map<String, List<MessageListener>> listenerByName = new HashMap<>();
    private String queueName;
    private String lockCollectionName = null;
    private String collectionName = null;

    private ThreadPoolExecutor threadPool;
    private ScheduledThreadPoolExecutor decouplePool;

    private boolean multithreadded = true;
    private int windowSize = 100;
    private boolean useChangeStream = true;
    private ChangeStreamMonitor changeStreamMonitor;
    private static Vector<StdMessaging> allMessagings = new Vector<>();

    //answers for messages
    private final Map<MorphiumId, Queue<Msg>> waitingForAnswers = new ConcurrentHashMap<>();
    private final Map<MorphiumId, CallbackRequest> waitingForCallbacks = new ConcurrentHashMap<>();

    private final BlockingQueue<ProcessingQueueElement> processing = new PriorityBlockingQueue<>();

    private final AtomicInteger requestPoll = new AtomicInteger(0);
    private final List<MorphiumId> idsInProgress = new Vector<>();
    private MessagingSettings settings = null;


    public StdMessaging() {
        allMessagings.add(this);
        id = UUID.randomUUID().toString();
        running = true;
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

        // listeners = new CopyOnWriteArrayList<>();
        // listenerByName = new HashMap<>();
        requestPoll.set(1);
    }
    /**
     * attaches to the default queue named "msg"
     *
     * @param m               - morphium
     * @param pause           - pause between checks
     * @param processMultiple - deprecated, set windowSize to 1 if needed
     */
    @Deprecated
    public StdMessaging(Morphium m, int pause, boolean processMultiple) {
        this(m, null, pause, processMultiple);

        if (!processMultiple) setWindowSize(1);
    }

    public StdMessaging(Morphium m, int pause) {
        this(m, null, pause, true, 10);
    }

    public StdMessaging(Morphium m) {
        this(m, null, 500, false, 100);
    }

    /**
     * @Deprecated - processMultiple is unused
     */
    @Deprecated
    public StdMessaging(Morphium m, int pause, boolean processMultiple, boolean multithreadded, int windowSize) {
        this(m, null, pause, multithreadded, windowSize);

        if (!processMultiple) setWindowSize(1);
    }

    public StdMessaging(Morphium m, int pause, boolean multithreadded, int windowSize) {
        this(m, null, pause, multithreadded, windowSize);
    }


    /**
     * @Deprecated - processMultiple is unused
     */
    @Deprecated
    public StdMessaging(Morphium m, String queueName, int pause, boolean processMultiple) {
        this(m, queueName, pause, false, m.getConfig().getMessagingWindowSize());

        if (!processMultiple) setWindowSize(1);
    }

    public StdMessaging(Morphium m, String queueName, int pause, boolean multithreadded, int windowSize) {
        this(m, queueName, pause, multithreadded, windowSize, m.isReplicaSet() || m.getDriver().getName().equals(InMemoryDriver.driverName));
    }

    /**
     * @Deprecated - processMultiple is unused
     */
    @Deprecated
    public StdMessaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize) {
        this(m, queueName, pause, multithreadded, windowSize, m.isReplicaSet() || m.getDriver().getName().equals(InMemoryDriver.driverName));

        if (!processMultiple) setWindowSize(1);
    }

    /**
     * @Deprecated - processMultiple is unused
     */
    @Deprecated
    public StdMessaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize, boolean useChangeStream) {
        this(m, queueName, pause, multithreadded, windowSize, useChangeStream);

        if (!processMultiple) setWindowSize(1);
    }


    /**
     * @param  m:              the morphium instance to use
     * @param queueName:       The name of the messaging queue
     * @param windowSize:      how many messages to mark for processing at once
     * @param useChangeStream: whether to use changeStream (reccommended!) or not. Attention: changestream cannot be used on single
     *                         mongodb instances or mongos sharded clusters - needs to be a replicaset!
     *                         morphium automatically sets this value accordingly depending on your configuration
     * @prarm pause: when waiting for incoming messages, especially when multithreadded == false, how long to wait between polls
     */
    public StdMessaging(Morphium m, String queueName, int pause, boolean multithreadded, int windowSize, boolean useChangeStream) {
        this();
        var cfg = m.getConfig().createCopy();
        cfg.messagingSettings().setMessageQueueName(queueName);
        cfg.messagingSettings().setMessagingPollPause(pause);
        cfg.messagingSettings().setMessagingMultithreadded(multithreadded);
        cfg.messagingSettings().setMessagingWindowSize(windowSize);
        cfg.messagingSettings().setUseChangeStrean(useChangeStream);
        init(m, cfg.messagingSettings());
    }


    public void init(Morphium m) {
        init(m, m.getConfig().createCopy().messagingSettings());
    }
    public void init(Morphium m, MessagingSettings settings) {
        morphium = m;
        this.settings = settings;
        statusInfoListenerEnabled = settings.isMessagingStatusInfoListenerEnabled();
        decouplePool = new ScheduledThreadPoolExecutor(windowSize, Thread.ofVirtual().name("decouple_thr-", 0).factory());

        if (settings.getMessagingStatusInfoListenerName() != null) {
            statusInfoListenerName = settings.getMessagingStatusInfoListenerName();
        }

        setWindowSize(settings.getMessagingWindowSize());
        setUseChangeStream(settings.isUseChangeStrean());
        setQueueName(settings.getMessageQueueName());
        setPause(settings.getMessagingPollPause());
        setMultithreadded(settings.isMessagingMultithreadded());
    }
    @Override
    public List<MorphiumMessaging> getAlternativeMessagings() {
        List<MorphiumMessaging> ret = new ArrayList<>(allMessagings);
        ret.remove(this);
        return ret;
    }

    @Override
    public void enableStatusInfoListener() {
        setStatusInfoListenerEnabled(true);
    }

    @Override
    public void disableStatusInfoListener() {
        setStatusInfoListenerEnabled(false);
    }

    @Override
    public String getStatusInfoListenerName() {
        return statusInfoListenerName;
    }

    @Override
    public void setStatusInfoListenerName(String statusInfoListenerName) {
        listenerByName.remove(this.statusInfoListenerName);
        this.statusInfoListenerName = statusInfoListenerName;
        listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
    }

    @Override
    public int getProcessingCount() {
        return processing.size();
    }

    @Override
    public int getInProgressCount() {
        return idsInProgress.size();
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
        return statusInfoListenerEnabled;
    }

    @Override
    public void setStatusInfoListenerEnabled(boolean statusInfoListenerEnabled) {
        this.statusInfoListenerEnabled = statusInfoListenerEnabled;

        if (statusInfoListenerEnabled && !listenerByName.containsKey(statusInfoListenerName)) {
            listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
        } else if (!statusInfoListenerEnabled) {
            listenerByName.remove(statusInfoListenerName);
        }
    }

    @Override
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

    @Override
    public List<String> getGlobalListeners() {
        List<MessageListener> localCopy = new ArrayList<>(listeners);
        List<String> ret = new ArrayList<>();

        for (MessageListener lst : localCopy) {
            ret.add(lst.getClass().getName());
        }

        return ret;
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

    private void initThreadPool() {
        threadPool = new ThreadPoolExecutor(
            settings.getThreadPoolMessagingCoreSize(),
            settings.getThreadPoolMessagingMaxSize(),
            settings.getThreadPoolMessagingKeepAliveTime(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            Thread.ofVirtual().name("msg-thr-", 0).factory()
        );
    }

    @Override
    public long getPendingMessagesCount() {
        Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName());
        q1.f(Msg.Fields.sender).ne(id).f("processed_by.0").notExists();
        return q1.countAll();
    }

    @Override
    public void removeMessage(Msg m) {
        morphium.delete(m, getCollectionName());
    }

    private boolean handleChangeStreamEvent(ChangeStreamEvent evt) {
        if (!running) {
            return false;
        }

        try {
            if (evt == null || evt.getOperationType() == null) {
                return running;
            }

            var id = ((Map) evt.getDocumentKey()).get("_id");

            if (id instanceof MorphiumId) {
                FindCommand fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(null));
                Map<String, Object> msg = null;

                try {
                    fnd.setFilter(Doc.of("_id", id));
                    fnd.setColl(getCollectionName()).setDb(morphium.getDatabase());
                    fnd.setProjection(Doc.of("_id", 1, "priority", 1, "sender", 1, "timestamp", 1));
                    List<Map<String, Object>> msgs = fnd.execute();
                    if (!msgs.isEmpty()) {
                        msg = msgs.get(0);
                    }
                }

                finally {
                    fnd.releaseConnection();
                }
                if (msg == null) return running;
                if (msg.get("sender").equals("id")) {
                    return running;
                }
                ProcessingQueueElement el = new ProcessingQueueElement();
                el.setPriority((Integer) msg.get("priority"));
                el.setId((MorphiumId) msg.get("_id"));
                el.setTimestamp((Long) msg.get("timestamp"));
                synchronized (processing) {
                    if (!processing.contains(el)) {
                        //                        log.info("ChangeStream: adding el "+el.getPriority()+"/"+el.getTimestamp());
                        processing.add(el);
                    }
                }
            }

            else {
                log.error("Some other id?!?!?" + id.getClass().getName());
            }
        }

        catch (Exception e) {
            log.error("Error during event processing in changestream", e);
        }
        return running;
    }

    private void initChangeStreams() {
        // pipeline for reducing incoming traffic
        List<Map<String, Object>> pipeline = new ArrayList<>();
        Map<String, Object> match = new LinkedHashMap<>();
        Map<String, Object> in = new LinkedHashMap<>();
        //        in.put("$eq", "insert"); //, "delete", "update"));
        in.put("$in", Arrays.asList("insert", "update"));
        match.put("operationType", in);
        pipeline.add(UtilsMap.of("$match", match));
        ChangeStreamMonitor lockMonitor = new ChangeStreamMonitor(morphium, getLockCollectionName(), false, pause, List.of(Doc.of("$match", Doc.of("operationType", Doc.of("$eq", "delete")))));
        lockMonitor.addListener(evt -> {
            //some lock removed
            requestPoll.incrementAndGet();
            return running;
        });
        changeStreamMonitor = new ChangeStreamMonitor(morphium, getCollectionName(), false, pause, pipeline);
        changeStreamMonitor.addListener(evt -> handleChangeStreamEvent(evt));
        changeStreamMonitor.start();
        lockMonitor.start();
    }

    @Override
    public void run() {
        setName("Msg " + id);

        if (statusInfoListenerEnabled) {
            listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
        }

        if (useChangeStream) {
            initChangeStreams();
        }

        // always run this find in addition to changestream
        try {
            AtomicLong lastRun = new AtomicLong(System.currentTimeMillis());
            final int pauseDings = getMorphium().getConfig().connectionSettings().getMaxWaitTime() / 3;
            decouplePool.scheduleWithFixedDelay(() -> {
                try {
                    if (requestPoll.get() > 0 || !useChangeStream || System.currentTimeMillis() - lastRun.get() > pauseDings) {
                        lastRun.set(System.currentTimeMillis());
                        morphium.inc(StatisticKeys.PULL);
                        StatisticValue sk = morphium.getStats().get(StatisticKeys.PULLSKIP);
                        sk.set(sk.get() + requestPoll.get());
                        requestPoll.set(0);
                        findMessages();
                    } else {
                        morphium.inc(StatisticKeys.SKIPPED_MSG_UPDATES);
                    }
                } catch (Throwable e) {
                    if (running) {
                        log.error("Unhandled exception " + e.getMessage(), e);
                    } else {
                        throw e;
                    }
                }

            }, pause, pause, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            log.error("Executor died?!?!");
        }

        //Main processing thread!
        while (running) {
            try {
                var prEl = processing.poll(1000, TimeUnit.MILLISECONDS);

                if (prEl == null) {
                    continue;
                }

                synchronized (processing) {
                    if (idsInProgress.contains(prEl.getId())) {
                        continue;
                    }

                    idsInProgress.add(prEl.getId());
                }

                Runnable r = () -> {
                    try {
                        var msg = morphium.findById(Msg.class, prEl.getId(), getCollectionName());
                        //message was deleted or dirty read
                        //dirty read only possible, because  msg read ReadPreferenceLevel is NEAREST

                        if (msg == null) {
                            var q = morphium.createQueryFor(Msg.class).setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY).f("_id").eq(prEl.getId());
                            q.setCollectionName(getCollectionName());
                            msg = q.get();//morphium.findById(Msg.class, prEl.getId(), getCollectionName());

                            if (msg == null) {
                                return;
                            }

                            log.debug("Msg==null =>diry read");
                        }

                        //do not process if no listener registered for this message
                        if (!msg.isAnswer() && !getListenerNames().containsKey(msg.getName()) && getGlobalListeners().isEmpty()) {
                            return;
                        }

                        //I sent the message
                        if (msg.getSender().equals(id)) {
                            return;
                        }

                        //exclusive message already processed
                        if (msg.isExclusive() && msg.getProcessedBy() != null && msg.getProcessedBy().size() != 0) {
                            return;
                        }

                        //I did already process this message
                        if (msg.getProcessedBy() != null && msg.getProcessedBy().contains(id)) {
                            return;
                        }

                        //recipient specified, but i am not it
                        if (msg.getRecipients() != null && !msg.getRecipients().contains(id)) {
                            return;
                        }

                        //Check answers
                        if (msg.isAnswer()) {
                            final Queue<Msg> answersForMessage = waitingForAnswers.get(msg.getInAnswerTo());

                            if (null != answersForMessage) {
                                // we're expecting this message!
                                updateProcessedBy(msg);

                                if (!answersForMessage.contains(msg)) {
                                    answersForMessage.add(msg);
                                }

                                checkDeleteAfterProcessing(msg);
                                return;
                            }

                            final CallbackRequest cbr = waitingForCallbacks.get(msg.getInAnswerTo());
                            final Msg theMessage = msg;

                            if (cbr != null) {
                                AsyncMessageCallback cb = cbr.callback;
                                Runnable cbRunnable = () -> {
                                    cb.incomingMessage(theMessage);
                                };
                                updateProcessedBy(theMessage);
                                queueOrRun(cbRunnable);

                                if (cbr.theMessage.isExclusive()) {
                                    waitingForCallbacks.remove(msg.getInAnswerTo());
                                }

                                checkDeleteAfterProcessing(msg);
                                return;
                            }
                        }

                        if (!getListenerNames().containsKey(msg.getName()) && getGlobalListeners().isEmpty()) {
                            return;
                        }

                        //really do handle message
                        if (msg.isExclusive()) {
                            lockAndProcess(msg);
                        } else {
                            processMessage(msg);
                        }
                    } finally {
                        synchronized (processing) {
                            idsInProgress.remove(prEl.getId());
                        }
                    }
                };
                queueOrRun(r);
            } catch (Exception e) {
                //swallow
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Messaging " + id + " stopped!");
        }

        listeners.clear();
        listenerByName.clear();
    }

    @Override
    public int getAsyncMessagesPending() {
        return waitingForCallbacks.size();
    }

    private void checkDeleteAfterProcessing(Msg obj) {
        if (obj.isDeleteAfterProcessing()) {
            if (obj.getDeleteAfterProcessingTime() == 0) {
                morphium.delete(obj, getCollectionName());
            } else {
                obj.setDeleteAt(new Date(System.currentTimeMillis() + obj.getDeleteAfterProcessingTime()));
                morphium.setInEntity(obj, getCollectionName(), Msg.Fields.deleteAt, obj.getDeleteAt());

                if (obj.isExclusive()) {
                    morphium.createQueryFor(MsgLock.class, getLockCollectionName()).f("_id").eq(obj.getMsgId()).set(MsgLock.Fields.deleteAt, obj.getDeleteAt());
                }
            }
        }
    }

    /**
     * pause processing for certain messages
     *
     * @param name
     */
    @Override
    public void pauseProcessingOfMessagesNamed(String name) {
        // log.debug("PAusing processing for "+name);
        pauseMessages.putIfAbsent(name, System.currentTimeMillis());
    }

    @Override
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
    @Override
    public Long unpauseProcessingOfMessagesNamed(String name) {
        if (!pauseMessages.containsKey(name)) {
            return 0L;
        }

        Long ret = pauseMessages.remove(name);

        if (ret != null) {
            ret = System.currentTimeMillis() - ret;
        }

        requestPoll.incrementAndGet();
        return ret;
    }

    private List<ProcessingQueueElement> getMessagesForProcessing() {
        if (!running) {
            return new ArrayList<>();
        }

        FindCommand fnd = null;

        try {
            Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName());

            if (listenerByName.isEmpty() && listeners.isEmpty()) {
                // No listeners - only answers will be processed
                return q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.inAnswerTo).in(waitingForAnswers.keySet()).limit(windowSize).idList();
            }

            // locking messages.. and getting broadcasts
            var idsToIgnore = morphium.createQueryFor(MsgLock.class).setCollectionName(getCollectionName() + "_lck").idList();

            synchronized (processing) {
                for (var p : processing) {
                    idsToIgnore.add(p.getId());
                }

                idsToIgnore.addAll(idsInProgress);
            }

            // q1: Exclusive messages, not locked yet, not processed yet
            var q1 = q.q().f(Msg.Fields.exclusive).eq(true).f("processed_by.0").notExists();
            // q2: non-exclusive messages, cannot be locked, not processed by me yet
            var q2 = q.q().f(Msg.Fields.exclusive).ne(true).f(Msg.Fields.processedBy).ne(id);
            q.f("_id").nin(idsToIgnore).f(Msg.Fields.sender).ne(id).f(Msg.Fields.recipients).in(Arrays.asList(null, id));
            Set<String> pausedMessagesKeys = pauseMessages.keySet();

            if (!pauseMessages.isEmpty()) {
                q.f(Msg.Fields.name).nin(pausedMessagesKeys);
            }

            q.or(q1, q2);
            // not searching for paused messages
            // Only handle messages we have listener for - not working, because of answers...
            q.setLimit(windowSize);
            q.sort(Msg.Fields.priority, Msg.Fields.timestamp);
            List<ProcessingQueueElement> queueElements = new ArrayList<>();
            // just trigger unprocessed messages for Changestream...
            int ws = windowSize;
            // get IDs of messages to process
            fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(Msg.class)));
            fnd.setDb(morphium.getDatabase());
            fnd.setFilter(q.toQueryObject());
            fnd.setProjection(Doc.of("_id", 1, "priority", 1, "timestamp", 1));
            fnd.setLimit(ws);
            fnd.setBatchSize(q.getBatchSize());
            fnd.setSort(q.getSort());
            fnd.setSkip(q.getSkip());
            fnd.setColl(getCollectionName());
            var result = fnd.execute();
            //to make it work with SingleMongoConnection!
            fnd.releaseConnection();
            fnd = null;

            if (!result.isEmpty()) {
                for (Map<String, Object> el : result) {
                    queueElements.add(new ProcessingQueueElement((Integer) el.get("priority"), (Long) el.get("timestamp"), (MorphiumId) el.get("_id")));
                }
            }

            if (q.countAll() != queueElements.size()) {
                //still messages left in mongodb for processing
                //or some messages were delete -> check to be sure
                requestPoll.incrementAndGet();
            }

            return queueElements;
        } catch (Exception e) {
            if (running) {
                log.error(id + ": Error while processing", e);
            }

            return null;
        } finally {
            if (fnd != null) {
                fnd.releaseConnection();
            }
        }
    }

    public void triggerCheck() {
        log.debug("Triggercheck called");
        requestPoll.incrementAndGet();
    }

    private void findMessages() {
        if (!running) {
            return;
        }

        // log.debug("getting messages...");
        List<ProcessingQueueElement> messages = getMessagesForProcessing();

        if (messages == null) {
            return;
        }

        if (messages.size() == 0) {
            return;
        }

        for (ProcessingQueueElement el : messages) {
            synchronized (processing) {
                if (!processing.contains(el) && !idsInProgress.contains(el)) {
                    processing.add(el);
                }

                int oldPrio = -10;
            }
        }
    }

    @SuppressWarnings("CommentedOutCode")
    private void lockAndProcess(Msg obj) {
        if (lockMessage(obj, id, obj.getDeleteAt())) {
            processMessage(obj);
        } else {
            requestPoll.incrementAndGet();
        }
    }

    public MsgLock getLock(Msg m) {
        return morphium.findById(MsgLock.class, m.getMsgId(), getLockCollectionName());
    }

    @Override
    public String getLockCollectionName() {
        if (lockCollectionName == null) {
            lockCollectionName = getCollectionName() + "_lck";
        }

        return lockCollectionName;
    }

    public boolean lockMessage(Msg m, String lockId) {
        return lockMessage(m, lockId, null);
    }

    public boolean lockMessage(Msg m, String lockId, Date delAt) {
        long start = System.currentTimeMillis();
        MsgLock lck = new MsgLock(m);
        lck.setLockId(lockId);

        if (delAt != null) {
            lck.setDeleteAt(delAt);
        }

        InsertMongoCommand cmd = null;

        try {
            cmd = new InsertMongoCommand(morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(MsgLock.class)));
            cmd.setColl(getLockCollectionName()).setDb(morphium.getDatabase()).setDocuments(List.of(morphium.getMapper().serialize(lck)));
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

    private void processMessage(Msg ms) {
        if (ms == null) {
            return;
        }

        var msg = morphium.reread(ms, getCollectionName());

        if (msg == null) {
            unlockIfExclusive(ms);
            return;
        }

        // I am the sender?
        if (msg.getSender().equals(getSenderId())) {
            log.error("This should have been filtered out before alreaday!!!");
            return;
        }

        // outdated message
        if (msg.isTimingOut() && msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
            // Delete outdated msg!
            log.debug(getSenderId() + ": Found outdated message - deleting it!");
            morphium.delete(msg, getCollectionName());
            unlockIfExclusive(msg);
            return;
        }

        if (msg.isExclusive() && msg.getProcessedBy().size() > 0) {
            return;
        }

        if (msg.getProcessedBy().contains(id)) {
            return;
        }

        if (listeners.isEmpty() && listenerByName.isEmpty()) {
            // message cannot be processed, as no listener is defined and message is no
            // answer.
            if (log.isDebugEnabled()) {
                log.debug("Not further processing - no listener for non answer message");
            }

            // removeProcessingFor(msg);
            unlockIfExclusive(msg);
            // log.info("not processing");
            return;
        }

        // Runnable r = ()->{
        boolean wasProcessed = false;
        boolean wasRejected = false;
        List<MessageRejectedException> rejections = new ArrayList<>();
        List<MessageListener> lst = new ArrayList<>(listeners);

        if (listenerByName.get(msg.getName()) != null) {
            lst.addAll(listenerByName.get(msg.getName()));
        }

        for (MessageListener l : lst) {
            try {
                if (pauseMessages.containsKey(msg.getName())) {
                    // paused - do not process
                    // log.warn("Received paused message?!?!? "+msg1.getMsgId());
                    // processing.remove(msg.getMsgId());
                    // removeProcessingFor(msg);
                    wasProcessed = false;
                    unlockIfExclusive(msg);
                    break;
                }

                if (l.markAsProcessedBeforeExec()) {
                    updateProcessedBy(msg);
                }

                Msg answer = l.onMessage(StdMessaging.this, msg);
                wasProcessed = true;

                if (autoAnswer && answer == null) {
                    answer = new Msg(msg.getName(), "received", "");
                }

                if (answer != null) {
                    msg.sendAnswer(StdMessaging.this, answer);

                    if (answer.getRecipients() == null) {
                        log.warn("Recipient of answer is null?!?!");
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
                requestPoll.incrementAndGet();
            } catch (Exception e) {
                log.error(id + ": listener Processing failed", e);
                checkDeleteAfterProcessing(msg);
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
                    log.error("No rejection  handler defined!!!");
                }
            }
        }

        if (wasProcessed && !msg.getProcessedBy().contains(id)) {
            updateProcessedBy(msg);
        }

        if (!wasRejected && wasProcessed) {
            checkDeleteAfterProcessing(msg);
        }

        unlockIfExclusive(msg);
    }

    private void unlockIfExclusive(Msg msg) {
        if (msg.isExclusive()) {
            // remove _own_ lock
            deleteLock(msg.getMsgId());
        }
    }

    private void deleteLock(MorphiumId msgId) {
        morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName()).f("_id").eq(msgId).f("lock_id").eq(id).remove();
    }

    // private void removeProcessingFor(Msg msg) {}
    private void updateProcessedBy(Msg msg) {
        if (msg == null) {
            return;
        }

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

    private void queueOrRun(Runnable r) {
        if (multithreadded) {
            boolean queued = false;

            while (!queued) {
                try {
                    // throtteling to windowSize - do not create more threads than windowSize
                    while (threadPool.getActiveCount() > windowSize) {
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

    @Override
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

    @Override
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

        requestPoll.incrementAndGet();
    }

    @Override
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

    @Override
    public String getSenderId() {
        return id;
    }

    @Override
    public StdMessaging setSenderId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public int getPause() {
        return pause;
    }

    @Override
    public StdMessaging setPause(int pause) {
        this.pause = pause;
        return this;
    }

    @Override
    public boolean isRunning() {
        if (useChangeStream) {
            return changeStreamMonitor != null && changeStreamMonitor.isRunning();
        }
        return running;
    }

    @Override
    public void terminate() {
        log.info("Terminate messaging");
        running = false;
        listenerByName.clear();
        listeners.clear();
        waitingForAnswers.clear();
        processing.clear();
        requestPoll.set(0);
        allMessagings.remove(this);
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
            if (retry > 2 * morphium.getConfig().connectionSettings().getMaxWaitTime() / 150 + 5) {
                throw new RuntimeException("Could not terminate Messaging! MaxTime exceeded twice");
            }
        }
    }

    @Override
    public void addMessageListener(MessageListener l) {
        if (listeners.contains(l)) {
            log.error("Cowardly refusing to add already registered listener");
        } else {
            listeners.add(l);
        }

        requestPoll.incrementAndGet();
    }

    @Override
    public void removeMessageListener(MessageListener l) {
        listeners.remove(l);
    }

    @Override
    public void queueMessage(final Msg m) {
        if (morphium.getDriver().getName().equals(SingleMongoConnectDriver.driverName)) {
            storeMsg(m, false);
        } else {
            storeMsg(m, true);
        }
    }

    @Override
    public void sendMessage(Msg m) {
        storeMsg(m, false);
    }

    @Override
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

    @Override
    public void sendMessageToSelf(Msg m) {
        sendMessageToSelf(m, false);
    }

    @Override
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

    @Override
    public boolean isAutoAnswer() {
        return autoAnswer;
    }

    @Override
    public MorphiumMessaging setAutoAnswer(boolean autoAnswer) {
        this.autoAnswer = autoAnswer;
        return this;
    }

    @Override
    public void onShutdown(Morphium m) {
        log.warn("Got shutdown event...");

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
            //e.printStackTrace();
            // swallow
        }
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
        if (!running) {
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

    private class CallbackRequest {
        Msg theMessage;
        AsyncMessageCallback callback;
        long ttl;
        long timestamp;
    }

    @Override
    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs, boolean throwExceptionOnTimeout) {
        if (!running) {
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
            while (running) {
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

                if (!running) {
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
    public boolean isProcessMultiple() {
        return windowSize == 1;
    }

    @Deprecated
    @Override
    public MorphiumMessaging setProcessMultiple(boolean processMultiple) {
        if (processMultiple) {
            windowSize = 10;
        } else {
            windowSize = 1;
        }

        return this;
    }

    @Override
    public String getQueueName() {
        return queueName;
    }

    @Override
    public MorphiumMessaging setQueueName(String queueName) {
        this.queueName = queueName;
        collectionName = null;
        lockCollectionName = null;
        return this;
    }

    @Override
    public boolean isMultithreadded() {
        return multithreadded;
    }

    @Override
    public MorphiumMessaging setMultithreadded(boolean multithreadded) {
        if (!multithreadded && threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        } else if (multithreadded && threadPool == null) {
            initThreadPool();
        }

        this.multithreadded = multithreadded;
        return this;
    }

    @Override
    public int getWindowSize() {
        return windowSize;
    }

    @Override
    public MorphiumMessaging setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    @Override
    public boolean isUseChangeStream() {
        return useChangeStream;
    }

    @Override
    public int getRunningTasks() {
        if (threadPool != null) {
            return threadPool.getActiveCount();
        }

        return 0;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    /**
     * Just for the understanding: if you do not use the changestream, messaging
     * will
     * poll for new mesages regularly (configured by the pause)
     * setPolling(true) == setUseChangeStream(false)!!!!!!
     **/
    @Override
    public MorphiumMessaging setPolling(boolean doPolling) {
        useChangeStream = !doPolling;
        return this;
    }

    @Override
    public MorphiumMessaging setUseChangeStream(boolean useChangeStream) {
        this.useChangeStream = useChangeStream;
        return this;
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

    public static class ProcessingQueueElement implements Comparable<ProcessingQueueElement> {
        private int priority;
        private MorphiumId id;
        private long timestamp;

        public ProcessingQueueElement() {
        }

        public ProcessingQueueElement(int priority, long timestamp, MorphiumId id) {
            this.priority = priority;
            this.id = id;
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public ProcessingQueueElement setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public int getPriority() {
            return priority;
        }

        public ProcessingQueueElement setPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public MorphiumId getId() {
            return id;
        }

        public ProcessingQueueElement setId(MorphiumId id) {
            this.id = id;
            return this;
        }

        @Override
        public int compareTo(ProcessingQueueElement o) {
            if (o.getPriority() < priority) return 1;

            if (o.getPriority() > priority) return -1;

            if (o.getTimestamp() < timestamp) return 1;

            if (o.getTimestamp() > timestamp) return -1;

            return o.getId().compareTo(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            ProcessingQueueElement that = (ProcessingQueueElement) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(priority, id, timestamp);
        }
    }

    public interface AsyncMessageCallback {
        void incomingMessage(Msg m);
    }
}
