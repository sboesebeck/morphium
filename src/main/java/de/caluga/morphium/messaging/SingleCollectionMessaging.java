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
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:48
 * <p>
 * Messaging implements a simple, threadsafe and messaging api. Used for cache
 * synchronization.
 * Msg can have several modes:
 * - LockedBy set to ALL (Exclusive Messages): every listener may process it (in
 * parallel), means 1->n. e.g. Cache sync
 * - LockedBy null (non exclusive messages): only one listener at a time
 * - Message listeners may return a Message as answer. Or throw a
 * MessageRejectedException.c
 */

@SuppressWarnings({ "ConstantConditions", "unchecked", "UnusedDeclaration", "UnusedReturnValue", "BusyWait" })
@Messaging(name = "StandardMessaging", description = "Standard message queueing implementation")
public class SingleCollectionMessaging extends Thread implements ShutdownListener, MorphiumMessaging {
    public final static String NAME = "StandardMessaging";
    private static final Logger log = LoggerFactory.getLogger(SingleCollectionMessaging.class);
    private final StatusInfoListener statusInfoListener = new StatusInfoListener();
    private String statusInfoListenerName = "morphium.status_info";
    private boolean statusInfoListenerEnabled = true;
    private Morphium morphium;
    private boolean running = true;
    private int pause = 100;

    private String id;
    private boolean autoAnswer = false;
    private String hostname;

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
    private static List<SingleCollectionMessaging> allMessagings = new java.util.concurrent.CopyOnWriteArrayList<>();

    // answers for messages
    private final Map<MorphiumId, Queue<Msg>> waitingForAnswers = new ConcurrentHashMap<>();
    private final Map<MorphiumId, CallbackRequest> waitingForCallbacks = new ConcurrentHashMap<>();

    private final BlockingQueue<ProcessingQueueElement> processing = new PriorityBlockingQueue<>();

    private final AtomicInteger requestPoll = new AtomicInteger(0);
    private final List<MorphiumId> idsInProgress = new java.util.concurrent.CopyOnWriteArrayList<>();
    // Debug counter for InMemoryDriver
    private final AtomicInteger changeStreamEventsReceived = new AtomicInteger(0);
    private MessagingSettings settings = null;
    private MessagingRegistry networkRegistry;


    public SingleCollectionMessaging() {
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
     * @Deprecated - use morphium.createMessaging() instead
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, int pause, boolean processMultiple) {
        this(m, null, pause, processMultiple);

        if (!processMultiple)
            setWindowSize(1);
    }

    /**
     * @Deprecated - use morphium.createMessaging() instead
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, int pause) {
        this(m, null, pause, true, 10);
    }

    /**
     * @Deprecated - use morphium.createMessaging() instead
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m) {
        this(m, null, 500, false, 100);
    }

    /**
     * @Deprecated - use morphium.createMessaging() instead
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, int pause, boolean processMultiple, boolean multithreadded,
                                     int windowSize) {
        this(m, null, pause, multithreadded, windowSize);

        if (!processMultiple)
            setWindowSize(1);
    }

    @Deprecated
    public SingleCollectionMessaging(Morphium m, int pause, boolean multithreadded, int windowSize) {
        this(m, null, pause, multithreadded, windowSize);
    }

    /**
     * @Deprecated - use morphium.createMessaging() instead
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, String queueName, int pause, boolean processMultiple) {
        this(m, queueName, pause, false, m.getConfig().getMessagingWindowSize());

        if (!processMultiple)
            setWindowSize(1);
    }

    @Deprecated
    public SingleCollectionMessaging(Morphium m, String queueName, int pause, boolean multithreadded, int windowSize) {
        this(m, queueName, pause, multithreadded, windowSize,
             m.isReplicaSet() || m.getDriver().getName().equals(InMemoryDriver.driverName));
    }

    /**
     * @Deprecated - use morphium.createMessaging() instead
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, String queueName, int pause, boolean processMultiple,
                                     boolean multithreadded, int windowSize) {
        this(m, queueName, pause, multithreadded, windowSize,
             m.isReplicaSet() || m.getDriver().getName().equals(InMemoryDriver.driverName));

        if (!processMultiple)
            setWindowSize(1);
    }

    /**
     * @Deprecated - processMultiple is unused
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, String queueName, int pause, boolean processMultiple,
                                     boolean multithreadded, int windowSize, boolean useChangeStream) {
        this(m, queueName, pause, multithreadded, windowSize, useChangeStream);

        if (!processMultiple)
            setWindowSize(1);
    }

    /**
     * @param m:               the morphium instance to use
     * @param queueName:       The name of the messaging queue
     * @param windowSize:      how many messages to mark for processing at once
     * @param useChangeStream: whether to use changeStream (reccommended!) or not.
     *                         Attention: changestream cannot be used on single
     *                         mongodb instances or mongos sharded clusters - needs
     *                         to be a replicaset!
     *                         morphium automatically sets this value accordingly
     *                         depending on your configuration
     * @param pause: when waiting for incoming messages, especially when
     *        multithreadded == false, how long to wait between polls
     */
    @Deprecated
    public SingleCollectionMessaging(Morphium m, String queueName, int pause, boolean multithreadded, int windowSize,
                                     boolean useChangeStream) {
        this();
        var cfg = m.getConfig().createCopy();
        cfg.messagingSettings().setMessageQueueName(queueName);
        cfg.messagingSettings().setMessagingPollPause(pause);
        cfg.messagingSettings().setMessagingMultithreadded(multithreadded);
        cfg.messagingSettings().setMessagingWindowSize(windowSize);
        cfg.messagingSettings().setUseChangeStream(useChangeStream);
        init(m, cfg.messagingSettings());
    }

    public void init(Morphium m) {
        init(m, m.getConfig().createCopy().messagingSettings());
    }

    public void init(Morphium m, MessagingSettings settings) {
        morphium = m;
        this.settings = settings;
        statusInfoListenerEnabled = settings.isMessagingStatusInfoListenerEnabled();
        decouplePool = new ScheduledThreadPoolExecutor(windowSize,
            Thread.ofVirtual().name("decouple_thr-", 0).factory());

        if (settings.getMessagingStatusInfoListenerName() != null) {
            statusInfoListenerName = settings.getMessagingStatusInfoListenerName();
        }

        setWindowSize(settings.getMessagingWindowSize());
        setUseChangeStream(settings.isUseChangeStream());
        setQueueName(settings.getMessageQueueName());
        setPause(settings.getMessagingPollPause());
        setMultithreadded(settings.isMessagingMultithreadded());

        morphium.ensureIndicesFor(Msg.class, getCollectionName());
        morphium.ensureIndicesFor(MsgLock.class, getLockCollectionName());

        if (settings.isMessagingRegistryEnabled()) {
            networkRegistry = new MessagingRegistry(this);
        }
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

    // @Override
    // public List<String> getGlobalListeners() {
    // List<MessageListener> localCopy = new ArrayList<>(listeners);
    // List<String> ret = new ArrayList<>();

    // for (MessageListener lst : localCopy) {
    // ret.add(lst.getClass().getName());
    // }

    // return ret;
    // }

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

    private void initThreadPool() {
        int coreSize = settings.getThreadPoolMessagingCoreSize();
        int maxSize = settings.getThreadPoolMessagingMaxSize();
        if (coreSize <= 0) {
            // Core size 0 leads to effectively single-threaded execution with an unbounded queue.
            // Interpret 0 as "auto" -> use max size for parallelism (virtual threads).
            coreSize = Math.max(1, maxSize);
        }
        threadPool = new ThreadPoolExecutor(
                        coreSize,
                        maxSize,
                        settings.getThreadPoolMessagingKeepAliveTime(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        Thread.ofVirtual().name("msg-thr-", 0).factory());
        threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS
                                      .toNanos((morphium.getConfig().driverSettings().getIdleSleepTime())));
                //Recursion!!!!
                log.info("Recursion!");
                executor.execute(r);
            }

        });
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

    // Use LinkedHashSet for O(1) contains and add operations (debug duplicate detection only)
    private final Set<Object> docIdsFromChangestreamSet = Collections.synchronizedSet(new LinkedHashSet<>());
    // Prevent duplicate listener invocation within the same JVM instance (especially important for InMem + change streams).
    private final Set<MorphiumId> locallyProcessedMessageIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private boolean handleChangeStreamEvent(ChangeStreamEvent evt) {
        // log.info("incoming CSE...");
        if (!running) {
            return false;
        }

        try {
            // if (evt == null || evt.getOperationType() == null) {
            // return running;
            // }

            var id = ((Map) evt.getDocumentKey()).get("_id");

            // Debug: Count ALL change stream events for InMemoryDriver
            // if (morphium.getDriver().getName().contains("InMem")) {
            //     int totalEvents = changeStreamEventsReceived.incrementAndGet();
            // if (totalEvents == 1 || totalEvents % 50 == 0 || totalEvents == 200) {
            //     log.info("{}: Change stream event #{} received", this.id, totalEvents);
            // }
            // }

            MorphiumId normalizedDocKeyId = null;
            if (id instanceof MorphiumId) {
                normalizedDocKeyId = (MorphiumId) id;
            } else if (id instanceof org.bson.types.ObjectId) {
                normalizedDocKeyId = new MorphiumId((org.bson.types.ObjectId) id);
            } else if (id instanceof String) {
                normalizedDocKeyId = new MorphiumId(id.toString());
            }

            // InMemoryDriver change streams may deliver duplicate insert events; filter them here.
            // Polling remains as a safety net, and this avoids double listener invocation.
            if (normalizedDocKeyId != null) {
                if (docIdsFromChangestreamSet.contains(normalizedDocKeyId)) {
                    return running;
                }

                docIdsFromChangestreamSet.add(normalizedDocKeyId);
                // Keep only recent 1000 IDs to prevent memory growth - clear when exceeded
                if (docIdsFromChangestreamSet.size() > 1000) {
                    docIdsFromChangestreamSet.clear();
                }
            }
            if (id instanceof MorphiumId || id instanceof org.bson.types.ObjectId || id instanceof String) {

                // Use fullDocument from change stream event instead of re-reading
                // The fullDocument is already a snapshot from the insert operation
                Map<String, Object> msg = evt.getFullDocument();

                if (msg == null) {
                    log.error("Msg is null from change stream fullDocument");
                    return running;
                }

                Object rawMsgId = msg.get("_id");
                MorphiumId messageId;
                if (rawMsgId instanceof MorphiumId) {
                    messageId = (MorphiumId) rawMsgId;
                } else if (rawMsgId instanceof org.bson.types.ObjectId) {
                    messageId = new MorphiumId((org.bson.types.ObjectId) rawMsgId);
                } else if (rawMsgId instanceof String) {
                    messageId = new MorphiumId(rawMsgId.toString());
                } else {
                    log.error("Unsupported _id type in change stream fullDocument: {}", rawMsgId == null ? "null" : rawMsgId.getClass().getName());
                    return running;
                }

                // Additional validation for exclusive messages
                Boolean exclusive = (Boolean) msg.get("exclusive");
                if (exclusive != null && exclusive) {
                    @SuppressWarnings("unchecked")
                    List<String> processedBy = (List<String>) (msg.containsKey("processed_by") ? msg.get("processed_by") : msg.get("processedBy"));
                    // Only skip if explicitly marked as processed by someone
                    if (processedBy != null && !processedBy.isEmpty()) {
                        // Exclusive message already processed, skip
                        log.error("Got already processed exclusive message");
                        return running;
                    }
                }

                // Check both processing queue and idsInProgress to prevent duplicates
                synchronized (processing) {
                    // First check if already in progress (most important for preventing duplicates)
                    if (idsInProgress.contains(messageId)) {
                        log.warn("CHANGESTREAM DUPLICATE CAUGHT: message {} already in idsInProgress", messageId);
                        return running;
                    }

                    // Create processing element
                    ProcessingQueueElement el = new ProcessingQueueElement();
                    Object prio = msg.get("priority");
                    if (prio instanceof Number) {
                        el.setPriority(((Number) prio).intValue());
                    } else {
                        el.setPriority(1000);
                    }
                    el.setId(messageId);
                    Object ts = msg.get("timestamp");
                    if (ts instanceof Number) {
                        el.setTimestamp(((Number) ts).longValue());
                    } else if (ts instanceof Date) {
                        el.setTimestamp(((Date) ts).getTime());
                    } else {
                        el.setTimestamp(System.currentTimeMillis());
                    }

                    // Check if not already queued for processing
                    if (!processing.contains(el)) {
                        processing.add(el);
                        // Add to idsInProgress immediately to prevent duplicate change stream events
                        // This must happen HERE, not in the processing thread, to close the race condition window
                        idsInProgress.add(messageId);

                        // log.debug("CHANGESTREAM: Queued message {} for processing", messageId);
                    } else {
                        log.warn("CHANGESTREAM DUPLICATE CAUGHT: Message {} already in processing queue", messageId);
                    }
                }
            } else {
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
        // in.put("$eq", "insert"); //, "delete", "update"));
        in.put("$in", Arrays.asList("insert"));
        match.put("operationType", in);
        pipeline.add(UtilsMap.of("$match", match));
        ChangeStreamMonitor lockMonitor = new ChangeStreamMonitor(morphium, getLockCollectionName(), false, pause,
            List.of(Doc.of("$match", Doc.of("operationType", Doc.of("$eq", "delete")))));
        lockMonitor.addListener(evt -> {
            // some lock removed
            if (morphium.createQueryFor(Msg.class, getCollectionName()).f("_id").eq(evt.getDocumentKey()).countAll() != 0) {
                // log.info("Lock CSE");
                requestPoll.incrementAndGet();
            }
            return running;
        });
        changeStreamMonitor = new ChangeStreamMonitor(morphium, getCollectionName(), false, pause, pipeline);
        changeStreamMonitor.addListener(evt -> handleChangeStreamEvent(evt));
        changeStreamMonitor.start();
        lockMonitor.start();
    }

    public void run() {
        setName("Msg " + id);

        if (statusInfoListenerEnabled) {
            listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
        }

        if (useChangeStream) {
            log.info("Changestream init");
            initChangeStreams();
        }

        // always run this find in addition to changestream
        try {
            AtomicLong lastRun = new AtomicLong(System.currentTimeMillis());
            decouplePool.scheduleWithFixedDelay(() -> {

                try {
                    // For InMemoryDriver or MorphiumServer with InMemory backend: always poll to ensure messages aren't missed
                    // Change streams through MorphiumServer may have timing issues with high concurrency
                    // For other drivers: only poll if requested or change streams disabled
                    boolean forcePolling = morphium.getDriver() != null && morphium.getDriver().isInMemoryBackend();
                    if (requestPoll.get() > 0 || !useChangeStream || forcePolling) {
                        // if (forcePolling || requestPoll.get() > 0) {
                        //     log.info("Polling (forced={}, requested={})", forcePolling, requestPoll.get() > 0);
                        // }
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

        // Main processing thread!
        while (running) {
            try {
                var prEl = processing.poll(1000, TimeUnit.MILLISECONDS);

                if (prEl == null) {
                    continue;
                }

                // Add to idsInProgress now that we've pulled it from the queue
                // For change stream messages, this is a duplicate add (already added in callback)
                // For polled messages, this is the first time we add it
                // Using synchronized to ensure thread safety
                synchronized (processing) {
                    if (!idsInProgress.contains(prEl.getId())) {
                        idsInProgress.add(prEl.getId());
                        // log.debug("PROCESSING: Added {} to idsInProgress (from queue)", prEl.getId());
                        // } else {
                        //     log.debug("PROCESSING: {} already in idsInProgress (from changestream)", prEl.getId());
                    }
                }

                final ProcessingQueueElement finalPrEl = prEl;
                Runnable r = () -> {
                    boolean wasProcessed = false;
                    Msg msg = null;
                    try {
                        if (!running || morphium == null || morphium.getDriver() == null) {
                            return;
                        }

                        msg = morphium.findById(Msg.class, finalPrEl.getId(), getCollectionName());
                        // message was deleted or dirty read
                        // dirty read only possible, because msg read ReadPreferenceLevel is NEAREST

                        if (msg == null) {
                            if (!running || morphium.getDriver() == null) {
                                return;
                            }
                            var q = morphium.createQueryFor(Msg.class)
                                            .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY).f("_id").eq(finalPrEl.getId());
                            q.setCollectionName(getCollectionName());
                            msg = q.get();// morphium.findById(Msg.class, finalPrEl.getId(), getCollectionName());

                            if (msg == null) {
                                return;
                            }

                            // log.debug("Msg!=null =>dirty read");
                        }

                        // do not process if no listener registered for this message
                        if (!msg.isAnswer() && !getListenerNames().containsKey(msg.getTopic())) {
                            return;
                        }

                        // Never receive messages sent by myself by default.
                        // sendMessageToSelf() uses sender="self" and explicit recipient, so it still works.
                        if (msg.getSender() != null && msg.getSender().equals(id)) {
                            return;
                        }

                        // exclusive message already processed
                        if (msg.isExclusive() && msg.getProcessedBy() != null && msg.getProcessedBy().size() != 0) {
                            return;
                        }

                        // I did already process this message
                        // For all drivers, check the stale copy first (fast path)
                        if (msg.getProcessedBy() != null && msg.getProcessedBy().contains(id)) {
                            return;
                        }

                        // recipient specified, but i am not it
                        if (msg.getRecipients() != null && !msg.getRecipients().contains(id)) {
                            return;
                        }

                        // Check answers
                        if (msg.isAnswer()) {
                            if (networkRegistry != null && msg.getTopic() != null && msg.getTopic().equals(getStatusInfoListenerName())) {
                                networkRegistry.updateFrom(msg);
                                checkDeleteAfterProcessing(msg);
                                return;
                            }

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

                        if (!getListenerNames().containsKey(msg.getTopic())) {
                            return;
                        }

                        // really do handle message
                        if (msg.isExclusive()) {
                            lockAndProcess(msg);
                            wasProcessed = true;
                        } else {
                            processMessage(msg);
                            wasProcessed = true;
                        }
                    } finally {
                        synchronized (processing) {
                            // Always remove from idsInProgress when done processing
                            // The database processed_by field handles duplicate prevention
                            idsInProgress.remove(finalPrEl.getId());
                        }
                    }
                };
                queueOrRun(r);
            } catch (Exception e) {
                // swallow
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Messaging " + id + " stopped!");
        }

        listenerByName.clear();
    }

    @Override
    public int getAsyncMessagesPending() {
        return waitingForCallbacks.size();
    }

    private void checkDeleteAfterProcessing(Msg obj) {
        if (!running || morphium == null || morphium.getConfig() == null || morphium.getDriver() == null) {
            return;
        }

        if (obj.isDeleteAfterProcessing()) {
            if (obj.getDeleteAfterProcessingTime() == 0) {
                morphium.delete(obj, getCollectionName());
            } else {
                obj.setDeleteAt(new Date(System.currentTimeMillis() + obj.getDeleteAfterProcessingTime()));
                String deleteAtField = morphium.getARHelper().getMongoFieldName(Msg.class, Msg.Fields.deleteAt.name());
                morphium.setInEntity(obj, getCollectionName(), deleteAtField, obj.getDeleteAt(), false, null);

                // MongoDB TTL cleanup is not instantaneous (monitor runs periodically); schedule best-effort cleanup.
                long deleteDelay = Math.max(0, obj.getDeleteAfterProcessingTime()) + TimeUnit.SECONDS.toMillis(1);
                if (decouplePool != null) {
                    decouplePool.schedule(() -> {
                        if (!running || morphium == null || morphium.getDriver() == null) {
                            return;
                        }

                        try {
                            morphium.createQueryFor(Msg.class, getCollectionName())
                                    .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                                    .f("_id").eq(obj.getMsgId())
                                    .remove();
                        } catch (Exception ignored) {
                        }
                    }, deleteDelay, TimeUnit.MILLISECONDS);
                }

                if (obj.isExclusive()) {
                    morphium.createQueryFor(MsgLock.class, getLockCollectionName()).f("_id").eq(obj.getMsgId())
                            .set(MsgLock.Fields.deleteAt, obj.getDeleteAt());

                    if (decouplePool != null) {
                        decouplePool.schedule(() -> {
                            if (!running || morphium == null || morphium.getDriver() == null) {
                                return;
                            }

                            try {
                                morphium.createQueryFor(MsgLock.class, getLockCollectionName())
                                        .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                                        .f("_id").eq(obj.getMsgId())
                                        .remove();
                            } catch (Exception ignored) {
                            }
                        }, deleteDelay, TimeUnit.MILLISECONDS);
                    }
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
    public void pauseTopicProcessing(String name) {
        // log.debug("PAusing processing for "+name);
        pauseMessages.putIfAbsent(name, System.currentTimeMillis());
    }

    @Override
    public List<String> getPausedTopics() {
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
    public Long unpauseTopicProcessing(String name) {
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

        // Check if morphium is still valid (not closed)
        if (morphium.getDriver() == null) {
            log.debug(id + ": Morphium driver is null, messaging system is shutting down");
            running = false;  // Stop the messaging loop
            return new ArrayList<>();
        }

        FindCommand fnd = null;

        try {
            Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName());

            if (listenerByName.isEmpty()) {
                // No listeners - only answers will be processed
                // Use stored field name explicitly for inMem parity (processed_by is persisted in snake_case)
                return q.q().f(Msg.Fields.sender).ne(id).f("processed_by").ne(id).f(Msg.Fields.inAnswerTo)
                       .in(waitingForAnswers.keySet()).limit(windowSize).idList();
            }

            // locking messages.. and getting broadcasts
            var idsToIgnore = morphium.createQueryFor(MsgLock.class).setCollectionName(getCollectionName() + "_lck")
                              .idList();

            synchronized (processing) {
                for (var p : processing) {
                    idsToIgnore.add(p.getId());
                }

                idsToIgnore.addAll(idsInProgress);

                // Debug logging for InMemoryDriver
                // if (morphium.getDriver().getName().contains("InMem")) {
                // log.info("POLLING DEBUG {}: processing.size={}, idsInProgress.size={}, idsToIgnore.size={}",
                // id, processing.size(), idsInProgress.size(), idsToIgnore.size());
                // }
            }

            // q1: Exclusive messages, not locked yet, not processed yet
            var q1 = q.q().f(Msg.Fields.exclusive).eq(true).f("processed_by.0").notExists();
            // q2: non-exclusive messages, cannot be locked, not processed by me yet
            // Use stored field name explicitly for inMem parity (processed_by is persisted in snake_case)
            var q2 = q.q().f(Msg.Fields.exclusive).ne(true).f("processed_by").ne(id);
            q.f("_id").nin(idsToIgnore);
            q.f(Msg.Fields.sender).ne(id);  // Don't receive messages sent by myself
            q.f(Msg.Fields.recipients).in(Arrays.asList(null, id));
            Set<String> pausedMessagesKeys = pauseMessages.keySet();

            if (!pauseMessages.isEmpty()) {
                // V5/V6 compatibility: check both 'topic' and 'name' fields
                q.f(Msg.Fields.topic).nin(pausedMessagesKeys);
                q.f("name").nin(pausedMessagesKeys);
            }

            q.or(q1, q2);
            // not searching for paused messages
            // Only handle messages we have listener for - not working, because of
            // answers...
            q.setLimit(windowSize);
            q.sort(Msg.Fields.priority, Msg.Fields.timestamp);
            List<ProcessingQueueElement> queueElements = new ArrayList<>();
            // just trigger unprocessed messages for Changestream...
            int ws = windowSize;
            // get IDs of messages to process
            fnd = new FindCommand(
                            morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(Msg.class)));
            fnd.setDb(morphium.getDatabase());
            fnd.setFilter(q.toQueryObject());
            fnd.setProjection(Doc.of("_id", 1, "priority", 1, "timestamp", 1));
            fnd.setLimit(ws);
            fnd.setBatchSize(q.getBatchSize());
            fnd.setSort(q.getSort());
            fnd.setSkip(q.getSkip());
            fnd.setColl(getCollectionName());
            var result = fnd.execute();
            // to make it work with SingleMongoConnection!
            fnd.releaseConnection();
            fnd = null;

            if (!result.isEmpty()) {
                for (Map<String, Object> el : result) {
                    el.putIfAbsent("priority", 100);
                    el.putIfAbsent("timestamp", System.currentTimeMillis());
                    queueElements.add(new ProcessingQueueElement((Integer) el.get("priority"),
                                      (Long) el.get("timestamp"), (MorphiumId) el.get("_id")));
                }
            }

            // Optimization: Avoid expensive countAll() on every poll
            // If we got exactly windowSize results, there might be more messages
            // If we got fewer, no need to poll again immediately
            if (queueElements.size() >= ws) {
                // Got full batch - likely more messages pending
                requestPoll.incrementAndGet();
            }

            // Debug logging for InMemoryDriver (skip expensive count in production)
            // if (log.isDebugEnabled() && morphium.getDriver().getName().contains("InMem")) {
            // long totalCount = q.countAll();
            // log.debug("POLLING RESULT {}: found {} messages, total in DB={}, idsToIgnore.size={}",
            //           id, queueElements.size(), totalCount, idsToIgnore.size());
            // }

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
        // log.debug("Triggercheck called");
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
                if (!processing.contains(el) && !idsInProgress.contains(el.getId())) {
                    processing.add(el);
                    // NOTE: We do NOT add to idsInProgress here
                    // It will be added when the processing thread pulls it from the queue
                    // This prevents messages from getting stuck in idsInProgress if never processed
                }
            }
        }
    }

    @SuppressWarnings("CommentedOutCode")
    private void lockAndProcess(Msg obj) {
        if (lockMessage(obj, id, obj.getDeleteAt())) {
            // Messages can be queued for processing before the exclusive lock exists.
            // If another instance already processed and unlocked, a stale queued message
            // could otherwise be processed again. Re-fetch to ensure processed_by state is current.
            Msg fresh = null;
            try {
                fresh = morphium.findById(Msg.class, obj.getMsgId(), getCollectionName());
            } catch (Exception ignored) {
            }

            if (fresh == null) {
                unlockIfExclusive(obj);
                return;
            }

            if (fresh.getProcessedBy() != null && !fresh.getProcessedBy().isEmpty()) {
                unlockIfExclusive(fresh);
                return;
            }

            processMessage(fresh);
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
        } else {
            lck.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl() * 2));
        }

        InsertMongoCommand cmd = null;

        try {
            cmd = new InsertMongoCommand(
                            morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(MsgLock.class)));
            cmd.setColl(getLockCollectionName()).setDb(morphium.getDatabase())
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

    private void processMessage(Msg msg) {
        if (msg == null) {
            return;
        }

        if (!running || morphium == null || morphium.getConfig() == null || morphium.getDriver() == null) {
            return;
        }

        // Per-instance dedup (broadcasts must not be delivered twice to same instance).
        // If a listener rejects a message, remove the local mark to allow retry.
        boolean locallyMarked = false;
        if (!msg.isExclusive()) {
            locallyMarked = locallyProcessedMessageIds.add(msg.getMsgId());
            if (!locallyMarked) {
                return;
            }
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

        boolean alreadyUpdatedProcessedBy = false;
        // Duplicate execution is prevented by queue/idsInProgress deduplication and the processed_by checks.
        // Avoid extra database roundtrips here (especially for InMemoryDriver), as they severely impact throughput.
        if (msg.getProcessedBy().contains(id)) {
            return;
        }

        if (listenerByName.isEmpty()) {
            // message cannot be processed, as no listener is defined and message is no
            // answer.
            // if (log.isDebugEnabled()) {
            //     log.debug("Not further processing - no listener for non answer message");
            // }

            // removeProcessingFor(msg);
            unlockIfExclusive(msg);
            // log.info("not processing");
            return;
        }

        // Runnable r = ()->{
        boolean wasProcessed = false;
        boolean wasRejected = false;
        List<MessageRejectedException> rejections = new ArrayList<>();
        List<MessageListener> lst = new ArrayList<>();

        if (listenerByName.get(msg.getTopic()) != null) {
            lst.addAll(listenerByName.get(msg.getTopic()));
        }

        for (MessageListener l : lst) {
            try {
                if (pauseMessages.containsKey(msg.getTopic())) {
                    // paused - do not process
                    // log.warn("Received paused message?!?!? "+msg1.getMsgId());
                    // processing.remove(msg.getMsgId());
                    // removeProcessingFor(msg);
                    wasProcessed = false;
                    unlockIfExclusive(msg);
                    break;
                }

                if (l.markAsProcessedBeforeExec() && !alreadyUpdatedProcessedBy) {
                    updateProcessedBy(msg);
                }

                // Call listener
                Msg answer = l.onMessage(SingleCollectionMessaging.this, msg);
                wasProcessed = true;

                if (autoAnswer && answer == null) {
                    answer = new Msg(msg.getTopic(), "received", "");
                }

                if (answer != null) {
                    msg.sendAnswer(SingleCollectionMessaging.this, answer);

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
            if (locallyMarked) {
                locallyProcessedMessageIds.remove(msg.getMsgId());
            }
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

        if (wasProcessed && !msg.getProcessedBy().contains(id) && !alreadyUpdatedProcessedBy) {
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
        morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName()).f("_id").eq(msgId)
                .f("lock_id").eq(id).remove();
    }

    // private void removeProcessingFor(Msg msg) {}
    /**
     * Atomically try to claim a message for processing by adding this instance's ID to processed_by.
     * Returns true ONLY if this instance successfully claimed it (i.e., we modified the document).
     * Returns false if already claimed by THIS instance (prevents duplicate processing within same instance).
     *
     * Note: For broadcast messages, multiple instances can claim the same message - this only prevents
     * the SAME instance from processing it twice.
     */
    private boolean tryClaimMessageForThisInstance(Msg msg) {
        if (msg == null) {
            return false;
        }

        if (msg.getProcessedBy().contains(id)) {
            return false; // Already claimed by THIS instance
        }

        Object queryId = msg.getMsgId();
        if (queryId instanceof MorphiumId) {
            queryId = new org.bson.types.ObjectId(((MorphiumId) queryId).getBytes());
        }
        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName());
        idq.f("_id").eq(queryId);
        // No need for additional query conditions - $addToSet is atomic and won't add duplicates
        // nModified will tell us if we actually added our ID (claimed it) or if it was already there

        Map<String, Object> qobj = idq.toQueryObject();
        Map<String, Object> set = Doc.of("processed_by", id);
        Map<String, Object> update = Doc.of("$addToSet", set);
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
            cmd.addUpdate(qobj, update, null, false, false, null, null, null);
            Map<String, Object> ret = cmd.execute();
            cmd.releaseConnection();
            cmd = null;

            // Check if we actually modified the document
            Object nModified = ret.get("nModified");
            if (nModified != null && ((Number) nModified).intValue() > 0) {
                // We successfully claimed it for THIS instance
                msg.getProcessedBy().add(id);
                return true;
            } else {
                // Our ID is already in processed_by (caught by the query condition)
                return false;
            }
        } catch (MorphiumDriverException e) {
            log.error("Error claiming message - will skip to avoid duplicates", e);
            return false;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    private boolean updateProcessedBy(Msg msg) {
        if (msg == null) {
            return false;
        }

        if (!running || morphium == null || morphium.getDriver() == null || morphium.getConfig() == null) {
            return false;
        }

        if (msg.getProcessedBy().contains(id)) {
            return true; // Already processed
        }

        Object queryId = msg.getMsgId();
        if (queryId instanceof MorphiumId) {
            queryId = new org.bson.types.ObjectId(((MorphiumId) queryId).getBytes());
        }
        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName());
        idq.f("_id").eq(queryId);
        // Don't modify local copy until database update succeeds
        Map<String, Object> qobj = idq.toQueryObject();
        Map<String, Object> set = Doc.of("processed_by", id);
        Map<String, Object> update = Doc.of("$addToSet", set);
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
            cmd.addUpdate(qobj, update, null, false, false, null, null, null);
            Map<String, Object> ret = cmd.execute();
            cmd.releaseConnection();
            cmd = null;

            // log.debug("Updating processed by for "+id+" on message "+msg.getMsgId());
            if (ret.get("nModified") == null && ret.get("modified") == null
                    || Integer.valueOf(0).equals(ret.get("nModified"))) {
                if (!running || morphium.getDriver() == null) {
                    return false;
                }

                try {
                    if (morphium.reread(msg, getCollectionName()) != null) {
                    if (!msg.getProcessedBy().contains(id)) {
                        log.warn(id + ": Could not update processed_by in msg " + msg.getMsgId());
                        log.warn(id + ": " + Utils.toJsonString(ret));
                        log.warn(id + ": msg: " + msg.toString());
                        return false; // Update failed
                    }

                    // } else {
                    // log.debug("message deleted by someone else!!!");
                }
                } catch (RuntimeException rte) {
                    // Can happen during shutdown when Morphium/driver is already closed
                    return false;
                }
                return true; // Already in processed_by (someone else updated it)
            } else {
                // Database update succeeded, now update local copy
                msg.getProcessedBy().add(id);
                return true;
            }
        } catch (MorphiumDriverException e) {
            log.error("Error updating processed by - this might lead to duplicate execution!", e);
            return false;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    private void queueOrRun(Runnable r) {
        if (multithreadded) {

            try {
                threadPool.execute(r);
            } catch (Throwable ignored) {
                ignored.printStackTrace();
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
    public String getCollectionName(String msgName) {
        return getCollectionName();
    }

    @Override
    public String getCollectionName(Msg msg) {
        return getCollectionName();
    }

    @Override
    public void addListenerForTopic(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            HashMap<String, List<MessageListener>> c = (HashMap) ((HashMap) listenerByName).clone();
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
    public void removeListenerForTopic(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            return;
        }

        HashMap<String, List<MessageListener>> c = (HashMap) ((HashMap) listenerByName).clone();
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
    public SingleCollectionMessaging setSenderId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public int getPause() {
        return pause;
    }

    @Override
    public SingleCollectionMessaging setPause(int pause) {
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
    public void close() {
        terminate();
    }

    @Override
    public void terminate() {
        log.info("Terminate messaging");
        if (networkRegistry != null) {
            networkRegistry.terminate();
        }
        running = false;
        listenerByName.clear();
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
        if (networkRegistry != null && (m.getTopic() == null || !m.getTopic().equals(getStatusInfoListenerName()))) {
            long registryWaitMs = TimeUnit.SECONDS.toMillis(Math.max(1, settings.getMessagingRegistryUpdateInterval()));
            if (settings.getMessagingRegistryCheckTopics() != MessagingSettings.TopicCheck.IGNORE && (m.getRecipients() == null || m.getRecipients().isEmpty())) {
                if (!networkRegistry.hasActiveListeners(m.getTopic())) {
                    networkRegistry.triggerDiscoveryAndWait(registryWaitMs);
                }
                if (!networkRegistry.hasActiveListeners(m.getTopic())) {
                    String msg = "No active listeners for topic '" + m.getTopic() + "'";
                    if (settings.getMessagingRegistryCheckTopics() == MessagingSettings.TopicCheck.WARN) {
                        log.warn(msg);
                    } else {
                        throw new MessageRejectedException(msg);
                    }
                }
            }
            if (settings.getMessagingRegistryCheckRecipients() != MessagingSettings.RecipientCheck.IGNORE && m.getRecipients() != null) {
                for (String r : m.getRecipients()) {
                    if (!networkRegistry.isParticipantActive(r)) {
                        networkRegistry.triggerDiscoveryAndWait(registryWaitMs);
                    }
                    if (!networkRegistry.isParticipantActive(r)) {
                        String msg = "Recipient '" + r + "' is not active";
                        if (settings.getMessagingRegistryCheckRecipients() == MessagingSettings.RecipientCheck.WARN) {
                            log.warn(msg);
                        } else {
                            throw new MessageRejectedException(msg);
                        }
                    }
                }
            }
        }

        AsyncOperationCallback cb = null;

        if (async) {
            // noinspection unused,unused
            cb = new AsyncOperationCallback() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query q, long duration, List result,
                                                 Object entity, Object... param) {
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query q, long duration, String error, Throwable t,
                                             Object entity, Object... param) {
                    log.error("Error storing msg", t);
                }
            };
        }

        m.setSender(id);
        m.setSenderHost(hostname);
        try {
            morphium.insert(m, getCollectionName(), cb);
            scheduleTimeoutDeletionIfNeeded(m, getCollectionName());
        } catch (RuntimeException e) {
            if (networkRegistry != null
                && settings.getMessagingRegistryCheckRecipients() == MessagingSettings.RecipientCheck.THROW
                && m.getRecipients() != null
                && !m.getRecipients().isEmpty()) {
                MessageRejectedException mre = new MessageRejectedException("Recipient '" + m.getRecipients().get(0) + "' is not active");
                mre.initCause(e);
                throw mre;
            }
            throw e;
        }
    }

    private void scheduleTimeoutDeletionIfNeeded(Msg msg, String collectionName) {
        if (msg == null || !running || decouplePool == null || morphium == null || morphium.getDriver() == null) {
            return;
        }

        if (!msg.isTimingOut() || msg.getTtl() <= 0 || msg.getMsgId() == null) {
            return;
        }

        long delay = msg.getTtl();

        try {
            Date deleteAt = msg.getDeleteAt();
            if (deleteAt != null) {
                delay = Math.max(0, deleteAt.getTime() - System.currentTimeMillis());
            }
        } catch (Exception ignored) {
        }

        long effectiveDelay = delay + TimeUnit.SECONDS.toMillis(1);

        decouplePool.schedule(() -> {
            if (!running || morphium == null || morphium.getDriver() == null) {
                return;
            }

            try {
                morphium.createQueryFor(Msg.class, collectionName)
                        .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                        .f("_id").eq(msg.getMsgId())
                        .remove();
            } catch (Exception ignored) {
            }
        }, effectiveDelay, TimeUnit.MILLISECONDS);
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
                LockSupport.parkNanos(2000000);
                // Thread.sleep(200);

                if (threadPool != null) {
                    threadPool.shutdownNow();
                }

                threadPool = null;
            }

            if (changeStreamMonitor != null) {
                changeStreamMonitor.terminate();
            }
        } catch (Exception e) {
            // e.printStackTrace();
            // swallow
        }
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
     * @param theMessage to be sent
     * @param timoutInMs - milliseconds to wait until listener is removed
     * @param cb - the message callback
     */
    @Override
    public <T extends Msg> void sendAndAwaitAsync(T theMessage, long timeoutInMs,
            SingleCollectionMessaging.AsyncMessageCallback cb) {
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
            while (running) {
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

                if (!running) {
                    throw new SystemShutdownException("Messaging shutting down - abort waiting!");
                }

                LockSupport.parkNanos(
                                TimeUnit.MILLISECONDS.toNanos(morphium.getConfig().driverSettings().getIdleSleepTime()));
            }
        } finally {
            List<T> answers = new ArrayList(waitingForAnswers.remove(requestMsgId));
            if (numberOfAnswers > 0 && answers.size() > numberOfAnswers) {
                returnValue = new ArrayList<>(answers.subList(0, numberOfAnswers));
            } else {
                returnValue = answers;
            }
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
        if (queueName != null && queueName.equals("msg")) {
            this.queueName = null;
        } else {
            this.queueName = queueName;
        }
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
            if (o.getPriority() < priority)
                return 1;

            if (o.getPriority() > priority)
                return -1;

            if (o.getTimestamp() < timestamp)
                return 1;

            if (o.getTimestamp() > timestamp)
                return -1;

            return o.getId().compareTo(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ProcessingQueueElement that = (ProcessingQueueElement) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    public interface AsyncMessageCallback {
        void incomingMessage(Msg m);
    }

    @Override
    public <T extends Msg> String getLockCollectionName(T topic) {
        return getLockCollectionName();
    }

    @Override
    public String getLockCollectionName(String topic) {
        return getLockCollectionName();
    }

    @Override
    public String getDMCollectionName(String sender) {
        return getCollectionName();

    }
}
