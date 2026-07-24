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
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.messaging.SingleCollectionMessaging.AsyncMessageCallback;
import de.caluga.morphium.messaging.SingleCollectionMessaging.MessageTimeoutException;
import de.caluga.morphium.messaging.SingleCollectionMessaging.ProcessingQueueElement;
import de.caluga.morphium.messaging.SingleCollectionMessaging.SystemShutdownException;
import de.caluga.morphium.query.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * <b>Beta / experimental (see {@link de.caluga.morphium.annotations.Beta}).</b> Behavior,
 * collection layout, or API surface may change without a deprecation cycle. Tracks
 * <a href="https://github.com/sboesebeck/morphium/issues/265">GitHub issue #265</a>.
 * <p>
 * Forked from {@link SingleCollectionMessaging} (a full clone, not a subclass/composition -
 * most of SCM's relevant members are {@code private}). SCM bugfixes may need manual porting
 * here; this class is deliberately isolated so that {@link SingleCollectionMessaging} and
 * {@link MultiCollectionMessaging} remain completely untouched by this feature.
 * <p>
 * <b>Mechanism.</b> Load tests showed that MongoDB messaging throughput is delivery-bound: a
 * single change-stream cursor delivers at a fixed cadence, capping request/reply well below
 * what the collection itself could sustain. {@code MultiCollectionMessaging} reaches higher
 * throughput not because of its per-topic collection split, but because answers/DMs run over a
 * dedicated per-recipient collection with its OWN change-stream cursor - parallelizing the
 * delivery path. {@code DualChannelMessaging} applies exactly that idea on top of an otherwise
 * unmodified {@link SingleCollectionMessaging} core: broadcast/topic traffic keeps using the main
 * collection and its single cursor (bit-identical to SCM, including window-limited polling and
 * the {@code idsInProgress}/{@code processing} backpressure guards), while directed messages
 * (recipients set) and their answers are routed through a second, per-recipient collection
 * ({@link #getDMCollectionName()}) watched by a second {@link ChangeStreamMonitor} and drained by
 * a dedicated dispatcher thread.
 * <p>
 * <b>Mixed-cluster behavior (deliberate, not an oversight).</b> There is no dual-read/dual-write
 * between the two collection layouts. A {@code DualChannelMessaging} instance can still receive
 * and answer requests from legacy {@code StandardMessaging} nodes on the same queue - the forked
 * main lane keeps SCM's answer-handling path as a compatibility side effect, intentionally not
 * removed. The reverse does NOT work: a {@code StandardMessaging} node awaiting an answer from a
 * {@code DualChannelMessaging} responder will time out, because the answer is written to the
 * requester's DM collection, which the legacy node never reads. This is accepted as the cost of
 * the beta opt-in - <b>all messaging participants on a given queue must run
 * {@code DualChannelMessaging} for DM/answer delivery to work</b> - and is logged at WARN on
 * startup (see {@link #run()}).
 */
@SuppressWarnings({ "ConstantConditions", "unchecked", "UnusedDeclaration", "UnusedReturnValue", "BusyWait" })
@Messaging(name = "DualChannelMessaging", description = "Beta: SingleCollectionMessaging fork with a dedicated per-recipient DM/answer collection and second change-stream cursor for parallelized delivery (#265)")
@de.caluga.morphium.annotations.Beta
public class DualChannelMessaging extends Thread implements ShutdownListener, MorphiumMessaging {
    public final static String NAME = "DualChannelMessaging";
    private static final Logger log = LoggerFactory.getLogger(DualChannelMessaging.class);
    private final StatusInfoListener statusInfoListener = new StatusInfoListener();
    private String statusInfoListenerName = "morphium.status_info";
    private boolean statusInfoListenerEnabled = true;
    private Morphium morphium;
    private volatile boolean running = true;
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
    private ChangeStreamMonitor lockChangeStreamMonitor;
    // Watchdog state for the main change stream — used to detect a stalled cursor
    // (events stop arriving while the fallback poll keeps finding unprocessed messages)
    // and trigger a restart without resume token to jump back to the present.
    private volatile long lastCsEventMs = 0;
    private volatile long lastCsRestartMs = 0;
    private final AtomicLong csStallRestarts = new AtomicLong(0);
    private List<Map<String, Object>> changeStreamPipeline;
    private int changeStreamMaxWait;
    // Throttles the main-thread-death log so we don't spam every poll cycle once detected.
    private volatile long lastMainThreadDeathLogMs = 0;
    private static List<DualChannelMessaging> allMessagings = new java.util.concurrent.CopyOnWriteArrayList<>();

    // answers for messages
    private final Map<MorphiumId, Queue<Msg>> waitingForAnswers = new ConcurrentHashMap<>();

    // Bounded trace of per-message processing decisions (skip reasons, matches, enqueues).
    // The processing pipeline bails out silently in several places, which made the recurring
    // answer-timeout flaky undiagnosable: a real occurrence showed the answer's change-stream
    // event arriving and delivery still failing with no hint why. Dumped only by
    // logAnswerTimeoutDiagnostics - zero log noise in normal operation.
    private static final int DECISION_TRACE_CAPACITY = 512;
    private final ArrayDeque<String> decisionTrace = new ArrayDeque<>();

    private void traceDecision(Object msgId, Object inAnswerTo, String decision) {
        String entry = System.currentTimeMillis() + " " + msgId
                       + (inAnswerTo != null ? " (answer to " + inAnswerTo + ")" : "") + ": " + decision;

        synchronized (decisionTrace) {
            decisionTrace.addLast(entry);

            if (decisionTrace.size() > DECISION_TRACE_CAPACITY) {
                decisionTrace.pollFirst();
            }
        }
    }

    /**
     * All traced processing decisions referencing the given message id - directly, or through a
     * linked message: an entry like "X (answer to REQ): queued" links answer X to request REQ,
     * and X's later entries (dequeue/runnable markers, which only know the element id) would
     * otherwise be invisible when filtering for REQ alone.
     */
    public List<String> getProcessingDecisions(MorphiumId msgId) {
        String idStr = String.valueOf(msgId);

        synchronized (decisionTrace) {
            Set<String> relevantIds = new HashSet<>();
            relevantIds.add(idStr);

            for (String e : decisionTrace) {
                if (e.contains(idStr)) {
                    // entry format: "<ts> <msgId> [(answer to <reqId>)]: <decision>" - the second
                    // token is the id of the message the decision was about
                    String[] parts = e.split(" ", 3);

                    if (parts.length >= 2) {
                        relevantIds.add(parts[1]);
                    }
                }
            }

            return decisionTrace.stream()
                   .filter(e -> relevantIds.stream().anyMatch(e::contains))
                   .collect(Collectors.toList());
        }
    }
    private final Map<MorphiumId, CallbackRequest> waitingForCallbacks = new ConcurrentHashMap<>();

    private final BlockingQueue<ProcessingQueueElement> processing = new PriorityBlockingQueue<>();

    private final AtomicInteger requestPoll = new AtomicInteger(0);
    private final List<MorphiumId> idsInProgress = new java.util.concurrent.CopyOnWriteArrayList<>();
    // Debug counter for InMemoryDriver
    private final AtomicInteger changeStreamEventsReceived = new AtomicInteger(0);
    private MessagingSettings settings = null;
    private MessagingRegistry networkRegistry;
    // Mongo field name of Msg.processedBy ("processed_by"), resolved once via the object mapper in init()
    private String processedByFieldName;

    // Ready signaling for tests - latch is counted down when change streams are fully initialized
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private volatile boolean ready = false;

    // ---------------------------------------------------------------------------------------
    // DualChannelMessaging (#265): dedicated per-recipient DM/answer collection + second cursor.
    // Everything above/below this block that is NOT in this block is the unmodified SCM main
    // lane (broadcast/topic traffic - main collection, one change stream, windowed poll,
    // idsInProgress/processing backpressure guards - kept bit-identical to SCM).
    // ---------------------------------------------------------------------------------------
    private ChangeStreamMonitor dmMonitor;
    private volatile long lastDmCsEventMs = 0;
    private final AtomicInteger requestDmPoll = new AtomicInteger(0);
    // Bounded processing queue for genuine directed messages (not answers - those are dispatched
    // inline from the CS listener, see handleDmAnswer). Overflow guard caps this at 2x windowSize
    // (see enqueueDmForProcessing) so a cursor backlog cannot grow this unbounded.
    private final BlockingQueue<ProcessingQueueElement> dmProcessing = new PriorityBlockingQueue<>();
    private final List<MorphiumId> dmIdsInProgress = new java.util.concurrent.CopyOnWriteArrayList<>();
    // Same InMemoryDriver-duplicate-event guard as the main lane's docIdsFromChangestreamSet.
    private final Set<Object> dmDocIdsFromChangestreamSet = Collections.synchronizedSet(new LinkedHashSet<>());
    private Thread dmDispatcherThread;
    private volatile boolean dmDispatcherRunning = false;


    public DualChannelMessaging() {
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

    // Only the no-arg constructor + init(Morphium[, MessagingSettings]) path is provided here.
    // SCM's legacy deprecated constructors (forRemoval=true, since 6.3) are not duplicated - the
    // registry-based Morphium#createMessaging() path (no-arg constructor + init()) is the only
    // construction API this class needs to support.

    public void init(Morphium m) {
        init(m, m.getConfig().createCopy().messagingSettings());
    }

    public void init(Morphium m, MessagingSettings settings) {
        morphium = m;
        this.settings = settings;
        processedByFieldName = morphium.getARHelper().getMongoFieldName(Msg.class, Msg.Fields.processedBy.name());
        statusInfoListenerEnabled = settings.isMessagingStatusInfoListenerEnabled();
        decouplePool = new ScheduledThreadPoolExecutor(windowSize,
            Thread.ofPlatform().name("decouple_thr-", 0).factory());

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

    /**
     * @return current depth of the DM lane's bounded processing queue (#265). Not part of
     * {@link MorphiumMessaging} - DM-lane specific, mainly for tests/diagnostics of the overflow
     * guard in {@link #enqueueDmForProcessing(MorphiumId, int, long)}.
     */
    public int getDmProcessingCount() {
        return dmProcessing.size();
    }

    /**
     * @return number of DM-lane messages currently claimed for processing (#265).
     */
    public int getDmInProgressCount() {
        return dmIdsInProgress.size();
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
                        Thread.ofPlatform().name("msg-thr-", 0).factory());
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
        q1.f(Msg.Fields.sender).ne(id).f(processedByFieldName + ".0").notExists();
        return q1.countAll();
    }

    @Override
    public void removeMessage(Msg m) {
        morphium.delete(m, getCollectionName());
    }

    // Use LinkedHashSet for O(1) contains and add operations (debug duplicate detection only)
    private final Set<Object> docIdsFromChangestreamSet = Collections.synchronizedSet(new LinkedHashSet<>());
    // Prevent duplicate listener invocation within the same JVM instance (especially important for InMem + change streams).
    // Maps message ID to timestamp when it was processed for TTL-based cleanup.
    private final Map<MorphiumId, Long> locallyProcessedMessageIds = new ConcurrentHashMap<>();
    // How long to keep message IDs tracked (30 minutes - should exceed any realistic message TTL)
    private static final long MESSAGE_TRACKING_RETENTION_MS = 30 * 60 * 1000;
    private boolean handleChangeStreamEvent(ChangeStreamEvent evt) {
        log.debug("CSE: {} incoming change stream event", this.id);
        if (!running) {
            return false;
        }

        try {
            // PoppyDB pushes synthetic "lock_released" events when a lock is deleted.
            // These have no documentKey/fullDocument - just trigger a re-poll.
            if ("lock_released".equals(evt.getOperationType())) {
                log.debug("CSE: {}: lock_released event received, triggering re-poll", this.id);
                requestPoll.incrementAndGet();
                return running;
            }

            // Requeue update (processedBy cleared, see pipeline): the message is pending
            // again but update events carry no fullDocument - trigger a poll to pick it up.
            if ("update".equals(evt.getOperationType())) {
                log.debug("CSE: {}: requeue update event received, triggering re-poll", this.id);
                requestPoll.incrementAndGet();
                return running;
            }

            var id = ((Map) evt.getDocumentKey()).get("_id");

            // Per-event counter, log demoted to debug (#264) - the flaky-hunt INFO version was
            // one line per change-stream event, far too chatty for production log levels.
            int totalEvents = changeStreamEventsReceived.incrementAndGet();

            if (log.isDebugEnabled()) {
                log.debug("CSE: {}: Change stream event #{} received, id={}", this.id, totalEvents, id);
            }

            MorphiumId normalizedDocKeyId = null;
            if (id instanceof MorphiumId) {
                normalizedDocKeyId = (MorphiumId) id;
            } else if (id instanceof org.bson.types.ObjectId) {
                normalizedDocKeyId = new MorphiumId((org.bson.types.ObjectId) id);
            } else if (id instanceof String) {
                normalizedDocKeyId = new MorphiumId(id.toString());
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
                    List<String> processedBy = (List<String>) msg.get(processedByFieldName);
                    // Only skip if explicitly marked as processed by someone
                    if (processedBy != null && !processedBy.isEmpty()) {
                        // Exclusive message already processed, skip
                        // NOTE: Do NOT add to docIdsFromChangestreamSet so polling can later process it
                        // if processedBy is cleared
                        traceDecision(messageId, msg.get("in_answer_to"), "cs-event: exclusive already processed by " + processedBy + ", skipped");
                        log.debug("Got already processed exclusive message - skipping but not marking as seen");
                        return running;
                    }
                }

                // InMemoryDriver change streams may deliver duplicate insert events; filter them here.
                // Polling remains as a safety net, and this avoids double listener invocation.
                // IMPORTANT: Only add to seen set AFTER processedBy check, so messages that are
                // initially skipped can be picked up by polling later if processedBy is cleared
                if (normalizedDocKeyId != null) {
                    if (docIdsFromChangestreamSet.contains(normalizedDocKeyId)) {
                        traceDecision(messageId, msg.get("in_answer_to"), "cs-event: duplicate event suppressed");
                        return running;
                    }

                    docIdsFromChangestreamSet.add(normalizedDocKeyId);
                    // Keep only recent 1000 IDs to prevent memory growth - clear when exceeded
                    if (docIdsFromChangestreamSet.size() > 1000) {
                        docIdsFromChangestreamSet.clear();
                    }
                }

                // Check both processing queue and idsInProgress to prevent duplicates
                synchronized (processing) {
                    // First check if already in progress (most important for preventing duplicates)
                    if (idsInProgress.contains(messageId)) {
                        traceDecision(messageId, msg.get("in_answer_to"), "cs-event: already in idsInProgress, skipped");
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

                        traceDecision(messageId, msg.get("in_answer_to"), "cs-event: queued for processing");
                        log.debug("CSE: {}: Queued message {} for processing, queue size={}", id, messageId, processing.size());
                    } else {
                        traceDecision(messageId, msg.get("in_answer_to"), "cs-event: already in processing queue, skipped");
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

    /**
     * Register this messaging collection with PoppyDB for optimizations.
     * Only effective when connected to a PoppyDB instance.
     */
    private void registerWithPoppyDB() {
        try {
            if (morphium.getDriver().isPoppyDB()) {
                log.info("Registering messaging collection with PoppyDB: {}", getCollectionName());
                Map<String, Object> cmdData = Doc.of(
                        "lockCollection", getLockCollectionName(),
                        "senderId", id
                                              );
                List<Map<String, Object>> result = morphium.runCommand(
                        "registerMessagingCollection", getCollectionName(), cmdData);
                if (result != null && !result.isEmpty() && Boolean.TRUE.equals(result.get(0).get("registered"))) {
                    log.info("Successfully registered messaging collection with PoppyDB: optimizations={}",
                             result.get(0).get("optimizations"));
                }
            }
        } catch (Exception e) {
            // Registration is optional - don't fail messaging if it doesn't work
            log.debug("Could not register messaging collection with PoppyDB: {}", e.getMessage());
        }
    }

    /**
     * Unregister this messaging subscriber from PoppyDB.
     */
    private void unregisterFromPoppyDB() {
        try {
            if (morphium.getDriver().isPoppyDB()) {
                log.debug("Unregistering messaging subscriber from PoppyDB: {}", id);
                Map<String, Object> cmdData = Doc.of("senderId", id);
                morphium.runCommand("unregisterMessagingSubscriber", getCollectionName(), cmdData);
            }
        } catch (Exception e) {
            log.debug("Could not unregister from PoppyDB: {}", e.getMessage());
        }
    }

    /**
     * Listener wrapper for the main change stream. Updates the liveness marker before
     * delegating to {@link #handleChangeStreamEvent(ChangeStreamEvent)} so the watchdog
     * can detect when the cursor stops delivering events.
     */
    private boolean onMainCsEvent(ChangeStreamEvent evt) {
        lastCsEventMs = System.currentTimeMillis();
        return handleChangeStreamEvent(evt);
    }

    /**
     * Restart the main change stream if events have stopped arriving while the
     * fallback poll is still finding unprocessed messages — the classic "cursor
     * fell behind" pattern. The fresh monitor starts without a resume token so it
     * jumps straight to the present; any backlog is picked up by the polling path,
     * which is idempotent w.r.t. already-processed messages.
     *
     * Only call from the fallback poll thread, never from a CS listener.
     */
    private void restartMainCsIfStalled(long stallThresholdMs) {
        if (!running || !useChangeStream) return;
        ChangeStreamMonitor old = changeStreamMonitor;
        if (old == null) return;

        long now = System.currentTimeMillis();
        long silenceMs = now - lastCsEventMs;
        if (silenceMs < stallThresholdMs) return;
        // Cooldown: don't restart again until the new stream had a fair chance
        // to either deliver an event or prove it is also stuck.
        if (now - lastCsRestartMs < stallThresholdMs) return;

        log.warn("Main change stream for '{}' silent for {}ms while polling found backlog — restarting (restart #{})",
                 getCollectionName(), silenceMs, csStallRestarts.incrementAndGet());

        try {
            old.terminate();
        } catch (Exception e) {
            log.warn("Error terminating stalled change stream for '{}': {}", getCollectionName(), e.getMessage());
        }

        try {
            ChangeStreamMonitor fresh = new ChangeStreamMonitor(
                    morphium, getCollectionName(), false, changeStreamMaxWait, changeStreamPipeline);
            fresh.addListener(this::onMainCsEvent);
            // same wiring as the original monitor: catch up once the fresh watch is up
            fresh.addWatchEstablishedListener(requestPoll::incrementAndGet);
            changeStreamMonitor = fresh;
            fresh.start();
            // Reset markers — give the fresh stream the full threshold before re-evaluating.
            lastCsEventMs = System.currentTimeMillis();
            lastCsRestartMs = lastCsEventMs;
        } catch (Exception e) {
            log.error("Failed to restart change stream for '{}'", getCollectionName(), e);
        }
    }

    /**
     * @return number of times the main change stream watchdog has triggered a restart since startup
     */
    public long getCsStallRestarts() {
        return csStallRestarts.get();
    }

    /**
     * Whether BOTH change streams (message collection + lock collection) are provably alive
     * and in sync: their watch loops receive a server reply at least every maxTimeMS (empty
     * batch heartbeat). A stream falling silent triggers an immediate fallback poll instead
     * of waiting for the regular interval. Public also for diagnostics/monitoring.
     */
    public boolean changeStreamsLive() {
        return changeStreamMonitor != null && changeStreamMonitor.isStreamLive()
            && lockChangeStreamMonitor != null && lockChangeStreamMonitor.isStreamLive();
    }

    /**
     * Detect the "main thread died but instance still appears alive" failure mode observed
     * in production: DualChannelMessaging is itself a Thread, and run() drains the
     * processing queue. If an Error escapes (pre-fix this was possible because the catch
     * was on Exception only) the thread terminates while the change stream cursor and
     * worker pools keep running — listeners get nothing, log goes silent.
     *
     * We can't safely re-start a Thread instance once it has terminated, so the best we
     * can do is fail loud: log at ERROR (throttled to every 30s) so the operator sees it
     * and can restart the application. {@link #isAlive()} reflects the messaging thread
     * itself; this method is invoked from the polling thread, so the check is meaningful.
     */
    private void checkMainThreadAlive() {
        if (!running) return;
        if (isAlive()) return;

        long now = System.currentTimeMillis();
        if (now - lastMainThreadDeathLogMs < 30_000) return;
        lastMainThreadDeathLogMs = now;
        log.error("FATAL: main messaging thread for '{}' is no longer alive but running=true. " +
                "Listeners will receive nothing until the application is restarted.",
                getCollectionName());
    }

    private void initChangeStreams() {
        // pipeline for reducing incoming traffic
        List<Map<String, Object>> pipeline = new ArrayList<>();
        Map<String, Object> match = new LinkedHashMap<>();
        Map<String, Object> in = new LinkedHashMap<>();
        // Accept "insert" (new messages), "lock_released" (PoppyDB pushes this when a lock
        // is deleted, so we can re-poll for exclusive messages without a separate connection)
        // and "update" (requeue detection, see the relevance filter below)
        in.put("$in", Arrays.asList("insert", "lock_released", "update"));
        match.put("operationType", in);
        pipeline.add(UtilsMap.of("$match", match));

        // Server-side relevance filter for insert events.
        //
        // Without this, every consumer's change stream cursor receives every insert into
        // the messaging collection — including messages addressed to other consumers and
        // the full payloads of large answers from other producers. Under high traffic
        // (e.g. document-export bursts) this causes the cursor to fall behind, producing
        // intermittent CS delivery delays where messages are only picked up via the
        // fallback poll (~FALLBACK_POLL_INTERVAL × pause latency).
        //
        // This filter restricts inserts to messages that are actually for this instance:
        //   - sender != my id        → don't echo my own inserts
        //   - recipients null/me     → broadcast or addressed to me
        // lock_released events are passed through unchanged (no fullDocument).
        // Use translated Mongo field names so the pipeline survives camelCase mapping changes.
        String senderField = "fullDocument." + morphium.getARHelper().getMongoFieldName(Msg.class, Msg.Fields.sender.name());
        String recipientsField = "fullDocument." + morphium.getARHelper().getMongoFieldName(Msg.class, Msg.Fields.recipients.name());
        Map<String, Object> insertRelevant = new LinkedHashMap<>();
        insertRelevant.put("operationType", "insert");
        insertRelevant.put(senderField, UtilsMap.of("$ne", id));
        insertRelevant.put("$or", Arrays.asList(
            UtilsMap.of(recipientsField, null),
            UtilsMap.of(recipientsField, id)
        ));
        // Requeue detection: clearing processedBy via a plain DB update makes a message
        // pending again but produces no insert event. The requeue signature is
        // updateDescription.updatedFields.processed_by set to an EMPTY array ($size 0) -
        // normal processing marks use positional keys (processed_by.0, ...) and stay filtered.
        String processedByField = morphium.getARHelper().getMongoFieldName(Msg.class, Msg.Fields.processedBy.name());
        Map<String, Object> requeueRelevant = new LinkedHashMap<>();
        requeueRelevant.put("operationType", "update");
        requeueRelevant.put("updateDescription.updatedFields." + processedByField, UtilsMap.of("$size", 0));
        Map<String, Object> relevanceMatch = new LinkedHashMap<>();
        relevanceMatch.put("$or", Arrays.asList(
            UtilsMap.of("operationType", "lock_released"),
            requeueRelevant,
            insertRelevant
        ));
        pipeline.add(UtilsMap.of("$match", relevanceMatch));
        // Use longer maxWait for change streams to avoid constant network polling
        // Change streams are designed to block server-side; short timeouts waste CPU/network
        changeStreamMaxWait = Math.max(pause * 10, morphium.getConfig().connectionSettings().getMaxWaitTime());
        changeStreamPipeline = pipeline;
        ChangeStreamMonitor lockMonitor = new ChangeStreamMonitor(morphium, getLockCollectionName(), false, changeStreamMaxWait,
            List.of(Doc.of("$match", Doc.of("operationType", Doc.of("$eq", "delete")))));
        lockChangeStreamMonitor = lockMonitor;
        lockMonitor.addListener(evt -> {
            // some lock removed
            if (morphium.createQueryFor(Msg.class, getCollectionName()).f("_id").eq(evt.getDocumentKey()).countAll() != 0) {
                // log.info("Lock CSE");
                requestPoll.incrementAndGet();
            }
            return running;
        });
        changeStreamMonitor = new ChangeStreamMonitor(morphium, getCollectionName(), false, changeStreamMaxWait, pipeline);
        changeStreamMonitor.addListener(this::onMainCsEvent);
        // On every watch (re-)establishment poll once: messages inserted while the stream
        // was down are invisible to the new stream unless a resume token was available.
        changeStreamMonitor.addWatchEstablishedListener(requestPoll::incrementAndGet);
        // Same for lock releases: a lock deleted during a lock-monitor gap would otherwise
        // never trigger its re-poll for exclusive messages.
        lockMonitor.addWatchEstablishedListener(requestPoll::incrementAndGet);
        // Initialize liveness markers so the watchdog doesn't fire immediately at startup.
        lastCsEventMs = System.currentTimeMillis();
        lastCsRestartMs = lastCsEventMs;

        // Start both monitors in parallel to speed up initialization
        Thread t1 = Thread.ofPlatform().start(() -> changeStreamMonitor.start());
        Thread t2 = Thread.ofPlatform().start(() -> lockMonitor.start());
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================================================
    // DualChannelMessaging (#265): second change-stream cursor for the per-recipient DM/answer
    // collection, its fast-path answer handling, and the dedicated dispatcher for genuine
    // directed (non-answer) messages. None of this touches the main lane above.
    // =========================================================================================

    /**
     * Sets up (and starts) the second {@link ChangeStreamMonitor}, watching this instance's own
     * DM collection ({@link #getDMCollectionName()}) with {@code fullDocument=true}. Mirrors the
     * requeue-detection pipeline shape of the main lane's monitor (see {@link #initChangeStreams()}).
     */
    private void initDmChangeStream() {
        Map<String, Object> requeueRelevant = new LinkedHashMap<>();
        requeueRelevant.put("operationType", "update");
        requeueRelevant.put("updateDescription.updatedFields." + processedByFieldName, UtilsMap.of("$size", 0));
        Map<String, Object> relevanceMatch = new LinkedHashMap<>();
        relevanceMatch.put("$or", Arrays.asList(
            UtilsMap.of("operationType", "insert"),
            requeueRelevant
        ));
        List<Map<String, Object>> pipeline = new ArrayList<>();
        pipeline.add(UtilsMap.of("$match", relevanceMatch));

        dmMonitor = new ChangeStreamMonitor(morphium, getDMCollectionName(), true, changeStreamMaxWait, pipeline);
        dmMonitor.addListener(this::onDmCsEvent);
        // Same reasoning as the main lane: on every watch (re-)establishment, poll once so
        // messages inserted while the DM stream was down (or before this instance even started)
        // are not stuck until the fallback poll interval.
        dmMonitor.addWatchEstablishedListener(requestDmPoll::incrementAndGet);
        lastDmCsEventMs = System.currentTimeMillis();
        dmMonitor.start();
    }

    /**
     * @return true if the DM change stream is provably alive (server heartbeat within maxTimeMS).
     * Extends {@link #changeStreamsLive()}'s notion of liveness to the DM lane.
     */
    public boolean dmChangeStreamLive() {
        return dmMonitor != null && dmMonitor.isStreamLive();
    }

    /**
     * Listener for the DM collection's change stream. Answers (inAnswerTo != null) are dispatched
     * INLINE, directly on this listener thread - registry status answers first, then delivery to
     * any waitingForAnswers/waitingForCallbacks entry, with the processed_by persist write pushed
     * off onto queueOrRun (see {@link #handleDmAnswer(Msg)} - same "dispatch before persist"
     * pattern as af932867, applied to the DM lane). Genuine directed messages (no inAnswerTo) are
     * NOT processed inline; they are handed to {@link #enqueueDmForProcessing(MorphiumId, int, long)}
     * for the dedicated dispatcher thread to pick up, with a bounded-queue overflow guard.
     */
    private boolean onDmCsEvent(ChangeStreamEvent evt) {
        lastDmCsEventMs = System.currentTimeMillis();

        if (!running) {
            return false;
        }

        try {
            if ("update".equals(evt.getOperationType())) {
                // requeue signature only (see pipeline) - no fullDocument on this event, just poll.
                requestDmPoll.incrementAndGet();
                return running;
            }

            if (!"insert".equals(evt.getOperationType())) {
                return running;
            }

            Map<String, Object> doc = evt.getFullDocument();

            if (doc == null) {
                requestDmPoll.incrementAndGet();
                return running;
            }

            Msg msg = morphium.getMapper().deserialize(Msg.class, doc);

            if (msg == null || msg.getMsgId() == null) {
                return running;
            }

            // InMemoryDriver change streams may deliver duplicate insert events; filter them here,
            // same as the main lane's docIdsFromChangestreamSet.
            if (dmDocIdsFromChangestreamSet.contains(msg.getMsgId())) {
                return running;
            }

            dmDocIdsFromChangestreamSet.add(msg.getMsgId());

            if (dmDocIdsFromChangestreamSet.size() > 1000) {
                dmDocIdsFromChangestreamSet.clear();
            }

            if (msg.isAnswer()) {
                handleDmAnswer(msg);
            } else {
                enqueueDmForProcessing(msg.getMsgId(), msg.getPriority(), msg.getTimestamp());
            }
        } catch (Exception e) {
            log.error("Error during DM change stream event processing for '{}'", getDMCollectionName(), e);
        }

        return running;
    }

    /**
     * Answer fast path (see {@link #onDmCsEvent(ChangeStreamEvent)}). Delivers to a waiting
     * caller/callback BEFORE persisting the processed_by mark - the majority-acked write is
     * pushed onto {@link #queueOrRun(Runnable)} instead of running inline on the CS listener
     * thread, exactly mirroring commit af932867's fix for the main lane's answer path.
     */
    private void handleDmAnswer(Msg m) {
        if (networkRegistry != null && m.getTopic() != null && m.getTopic().equals(getStatusInfoListenerName())) {
            networkRegistry.updateFrom(m);
            checkDeleteAfterProcessingDm(m);
            return;
        }

        final Queue<Msg> answersForMessage = waitingForAnswers.get(m.getInAnswerTo());

        if (answersForMessage != null) {
            if (!m.getProcessedBy().contains(id)) {
                m.getProcessedBy().add(id);
            }

            if (!answersForMessage.contains(m)) {
                answersForMessage.add(m);
            }

            queueOrRun(() -> persistDmProcessedByMark(m));
            checkDeleteAfterProcessingDm(m);
            return;
        }

        final CallbackRequest cbr = waitingForCallbacks.get(m.getInAnswerTo());

        if (cbr != null) {
            AsyncMessageCallback cb = cbr.callback;

            if (!m.getProcessedBy().contains(id)) {
                m.getProcessedBy().add(id);
            }

            queueOrRun(() -> cb.incomingMessage(m));
            queueOrRun(() -> persistDmProcessedByMark(m));

            if (cbr.theMessage.isExclusive()) {
                waitingForCallbacks.remove(m.getInAnswerTo());
            }

            checkDeleteAfterProcessingDm(m);
            return;
        }

        // No one is waiting (yet, or anymore - the request may have timed out). Fall back to the
        // normal dispatcher path: a re-read there re-evaluates waiters/callbacks once more and,
        // failing that, falls through to topic listener dispatch (an answer can double as a
        // regular message if a listener is registered for its topic).
        enqueueDmForProcessing(m.getMsgId(), m.getPriority(), m.getTimestamp());
    }

    /**
     * Bounded enqueue for the DM dispatcher. Overflow guard: if the queue already holds more than
     * 2x windowSize elements, the event is dropped and only the poll trigger is incremented - the
     * next fallback poll (window-limited, same as the main lane) will pick the backlog up instead
     * of letting an unbounded cursor backlog grow this queue forever.
     */
    private void enqueueDmForProcessing(MorphiumId messageId, int priority, long timestamp) {
        synchronized (dmProcessing) {
            if (dmIdsInProgress.contains(messageId)) {
                return;
            }

            if (dmProcessing.size() > 2L * windowSize) {
                requestDmPoll.incrementAndGet();
                return;
            }

            ProcessingQueueElement el = new ProcessingQueueElement(priority, timestamp, messageId);

            if (!dmProcessing.contains(el)) {
                dmProcessing.add(el);
                dmIdsInProgress.add(messageId);
            }
        }
    }

    /**
     * Dedicated dispatcher thread for the DM lane, started from {@link #run()}. Drains
     * {@link #dmProcessing} and re-reads/dispatches each element against the DM collection. Unlike
     * the main lane's processing loop, there is no locking/MsgLock involved - the DM collection has
     * exactly one consumer (this instance), so exclusivity is moot there.
     */
    private void dmDispatcherLoop() {
        while (dmDispatcherRunning) {
            try {
                ProcessingQueueElement prEl = dmProcessing.poll(1000, TimeUnit.MILLISECONDS);

                if (prEl == null) {
                    continue;
                }

                processDmElement(prEl);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                if (dmDispatcherRunning) {
                    log.error("Unhandled throwable in DM dispatcher loop for '{}' — keeping thread alive",
                            getDMCollectionName(), t);
                }
            }
        }
    }

    private void processDmElement(ProcessingQueueElement prEl) {
        try {
            if (!running || morphium == null || morphium.getDriver() == null) {
                return;
            }

            var q = morphium.createQueryFor(Msg.class)
                    .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY).f("_id").eq(prEl.getId());
            q.setCollectionName(getDMCollectionName());
            Msg msg = q.get();

            if (msg == null) {
                return;
            }

            if (msg.getProcessedBy() != null && msg.getProcessedBy().contains(id)) {
                return;
            }

            if (msg.isAnswer()) {
                if (networkRegistry != null && msg.getTopic() != null && msg.getTopic().equals(getStatusInfoListenerName())) {
                    networkRegistry.updateFrom(msg);
                    checkDeleteAfterProcessingDm(msg);
                    return;
                }

                final Queue<Msg> answersForMessage = waitingForAnswers.get(msg.getInAnswerTo());

                if (answersForMessage != null) {
                    if (!msg.getProcessedBy().contains(id)) {
                        msg.getProcessedBy().add(id);
                    }

                    if (!answersForMessage.contains(msg)) {
                        answersForMessage.add(msg);
                    }

                    persistDmProcessedByMark(msg);
                    checkDeleteAfterProcessingDm(msg);
                    return;
                }

                final CallbackRequest cbr = waitingForCallbacks.get(msg.getInAnswerTo());

                if (cbr != null) {
                    final Msg theMessage = msg;

                    if (!theMessage.getProcessedBy().contains(id)) {
                        theMessage.getProcessedBy().add(id);
                    }

                    queueOrRun(() -> cbr.callback.incomingMessage(theMessage));
                    persistDmProcessedByMark(theMessage);

                    if (cbr.theMessage.isExclusive()) {
                        waitingForCallbacks.remove(msg.getInAnswerTo());
                    }

                    checkDeleteAfterProcessingDm(msg);
                    return;
                }

                // no waiter/callback (anymore) - fall through to topic listener dispatch below,
                // same as the main lane's equivalent fallthrough.
            }

            if (!getListenerNames().containsKey(msg.getTopic())) {
                return;
            }

            processDmMessage(msg);
        } finally {
            synchronized (dmProcessing) {
                dmIdsInProgress.remove(prEl.getId());
            }
        }
    }

    /**
     * Dispatch a genuine directed message to its topic listener(s) against the DM collection.
     * Exception in a listener leaves processed_by empty so the fallback poll redelivers it (until
     * TTL expiry); {@link MessageRejectedException} triggers a DM poll and invokes its rejection
     * handler, mirroring SCM's semantics for the main lane.
     */
    private void processDmMessage(Msg msg) {
        if (msg == null || !running || morphium == null || morphium.getConfig() == null || morphium.getDriver() == null) {
            return;
        }

        if (msg.getProcessedBy() != null && msg.getProcessedBy().contains(id)) {
            return;
        }

        if (listenerByName.isEmpty()) {
            return;
        }

        List<MessageListener> lst = new ArrayList<>();

        if (listenerByName.get(msg.getTopic()) != null) {
            lst.addAll(listenerByName.get(msg.getTopic()));
        }

        boolean wasProcessed = false;
        boolean wasRejected = false;
        List<MessageRejectedException> rejections = new ArrayList<>();

        for (MessageListener l : lst) {
            try {
                if (pauseMessages.containsKey(msg.getTopic())) {
                    requestDmPoll.incrementAndGet();
                    return;
                }

                if ((l.markAsProcessedBeforeExec() || msg.isExclusive()) && !msg.getProcessedBy().contains(id)) {
                    persistDmProcessedByMark(msg);
                    msg.getProcessedBy().add(id);
                }

                Msg answer = l.onMessage(this, msg);
                wasProcessed = true;

                if (autoAnswer && answer == null) {
                    answer = new Msg(msg.getTopic(), "received", "");
                }

                if (answer != null) {
                    msg.sendAnswer(this, answer);
                }
            } catch (MessageRejectedException mre) {
                log.info(id + ": DM message was rejected by listener: " + mre.getMessage());
                wasRejected = true;
                rejections.add(mre);
                requestDmPoll.incrementAndGet();
            } catch (Exception e) {
                log.error(id + ": DM listener processing failed", e);
                checkDeleteAfterProcessingDm(msg);
            }
        }

        if (wasRejected) {
            for (MessageRejectedException mre : rejections) {
                if (mre.getRejectionHandler() != null) {
                    try {
                        mre.getRejectionHandler().handleRejection(this, msg);
                    } catch (Exception e) {
                        log.error("Error in DM rejection handling", e);
                    }
                } else {
                    log.error("No rejection handler defined for rejected DM message!");
                }
            }

            // processed_by stays empty (unless a prior markAsProcessedBeforeExec listener already
            // set it) so the fallback poll can redeliver.
            return;
        }

        if (wasProcessed && !msg.getProcessedBy().contains(id)) {
            persistDmProcessedByMark(msg);
            msg.getProcessedBy().add(id);
        }

        if (wasProcessed) {
            checkDeleteAfterProcessingDm(msg);
        }
    }

    /**
     * DB-only $addToSet write for the DM collection - companion to the main lane's
     * {@link #persistProcessedByMark(Msg)}, same fire-and-forget semantics (idempotent, no
     * local-contains shortcut since the caller already mutated the local list).
     */
    private void persistDmProcessedByMark(Msg msg) {
        if (msg == null || !running || morphium == null || morphium.getDriver() == null || morphium.getConfig() == null) {
            return;
        }

        Object queryId = msg.getMsgId();

        if (queryId instanceof MorphiumId) {
            queryId = new org.bson.types.ObjectId(((MorphiumId) queryId).getBytes());
        }

        String dmColl = getDMCollectionName();
        Query<Msg> idq = morphium.createQueryFor(Msg.class, dmColl);
        idq.f("_id").eq(queryId);
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(dmColl).setDb(morphium.getDatabase());
            cmd.addUpdate(idq.toQueryObject(), Doc.of("$addToSet", Doc.of(processedByFieldName, id)),
                          null, false, false, null, null, null);
            cmd.execute();
            cmd.releaseConnection();
            cmd = null;
        } catch (MorphiumDriverException e) {
            log.error("Error persisting processed_by mark for DM message " + msg.getMsgId(), e);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    /**
     * DM-collection companion to the main lane's {@link #checkDeleteAfterProcessing(Msg)}. No
     * MsgLock handling - the DM lane never locks (single consumer).
     */
    private void checkDeleteAfterProcessingDm(Msg obj) {
        if (!running || morphium == null || morphium.getConfig() == null || morphium.getDriver() == null) {
            return;
        }

        if (!obj.isDeleteAfterProcessing()) {
            return;
        }

        String coll = getDMCollectionName();

        if (obj.getDeleteAfterProcessingTime() == 0) {
            morphium.delete(obj, coll);
            return;
        }

        obj.setDeleteAt(new Date(System.currentTimeMillis() + obj.getDeleteAfterProcessingTime()));
        String deleteAtField = morphium.getARHelper().getMongoFieldName(Msg.class, Msg.Fields.deleteAt.name());
        morphium.setInEntity(obj, coll, deleteAtField, obj.getDeleteAt(), false, null);
        long deleteDelay = Math.max(0, obj.getDeleteAfterProcessingTime()) + TimeUnit.SECONDS.toMillis(1);

        if (decouplePool != null) {
            decouplePool.schedule(() -> {
                if (!running || morphium == null || morphium.getDriver() == null) {
                    return;
                }

                try {
                    morphium.createQueryFor(Msg.class, coll)
                            .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                            .f("_id").eq(obj.getMsgId())
                            .remove();
                } catch (Exception ignored) {
                }
            }, deleteDelay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * DM-collection poll fallback + backlog scan, mirroring the main lane's windowed poll query
     * shape ({@link #getMessagesForProcessing()}) but simpler: no lock-collection join, no
     * exclusive/lock branching (single consumer). Called from the same decouplePool tick as the
     * main lane's fallback poll (see {@link #run()}).
     *
     * @return true if at least one unprocessed DM was found (backlog signal for the DM stall watchdog)
     */
    private boolean findDmMessages() {
        if (!running) {
            return false;
        }

        try {
            var idsToIgnore = new ArrayList<MorphiumId>();

            synchronized (dmProcessing) {
                for (var p : dmProcessing) {
                    idsToIgnore.add(p.getId());
                }

                idsToIgnore.addAll(dmIdsInProgress);
            }

            Query<Msg> q = morphium.createQueryFor(Msg.class, getDMCollectionName());
            q.f(processedByFieldName + ".0").notExists();

            if (!idsToIgnore.isEmpty()) {
                q.f("_id").nin(idsToIgnore);
            }

            q.setLimit(windowSize);
            q.sort(Msg.Fields.priority, Msg.Fields.timestamp);
            List<Msg> result = q.asList();

            if (result.isEmpty()) {
                return false;
            }

            for (Msg m : result) {
                enqueueDmForProcessing(m.getMsgId(), m.getPriority(), m.getTimestamp());
            }

            if (result.size() >= windowSize) {
                requestDmPoll.incrementAndGet();
            }

            return true;
        } catch (Exception e) {
            if (running) {
                log.error(id + ": Error while polling DM collection", e);
            }

            return false;
        }
    }

    /**
     * Registry-gated orphan sweep (#265). Setting name is
     * {@link MessagingSettings#isMessagingDmCleanupOrphansOnStartup()} despite running
     * periodically (on the same decouplePool tick as the fallback polls, see {@link #run()}) -
     * "OnStartup" refers to it being on by default from the first startup onward, not to a
     * one-shot execution. Drops per-recipient DM collections that are BOTH empty AND belong to
     * a participant the registry currently considers inactive. Never touches this instance's own
     * collection, and never touches a non-empty collection - an empty collection momentarily
     * belonging to a live participant between messages must keep its TTL index.
     * <p>
     * <b>Known limitation (needs operator sign-off, see final report):</b> liveness is derived
     * from {@link MessagingRegistry#isParticipantActive(String)}, which only reports a
     * participant "active" if it currently has at least one registered topic listener. A pure
     * requester (no listeners, only ever sends/awaits answers) is therefore reported inactive
     * even while alive, and its currently-empty DM collection could be swept - causing a
     * change-stream cursor disruption on that peer and loss of its TTL index until restart. This
     * is only safe in clusters where every DualChannelMessaging participant also registers at
     * least one topic listener (e.g. the built-in status-info listener, if enabled).
     */
    private void sweepOrphanDmCollections() {
        if (networkRegistry == null) {
            return;
        }

        try {
            String ownDmCollection = getDMCollectionName();
            String prefix = getCollectionName() + "_dm_";
            // Server-side filtered via a regex pattern (Morphium.listCollections(pattern) caches
            // results as CollectionInfo, which needs an @Id - see CollectionInfo#name).
            List<String> all = morphium.listCollections(java.util.regex.Pattern.quote(prefix) + ".*");

            if (all == null) {
                return;
            }

            for (String coll : all) {
                if (coll == null || coll.equals(ownDmCollection) || !coll.startsWith(prefix)) {
                    continue;
                }

                String candidateSenderId = coll.substring(prefix.length());

                if (networkRegistry.isParticipantActive(candidateSenderId)) {
                    continue; // considered alive - never touch
                }

                try {
                    long count = morphium.createQueryFor(Msg.class, coll).countAll();

                    if (count == 0) {
                        log.info("DualChannelMessaging orphan sweep: dropping empty, inactive DM collection '{}'", coll);
                        morphium.dropCollection(Msg.class, coll, null);
                    }
                } catch (Exception e) {
                    log.debug("Orphan DM sweep: error checking/dropping '{}': {}", coll, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.debug("Orphan DM collection sweep failed: {}", e.getMessage());
        }
    }

    public void run() {
        setName("Msg " + id);

        if (statusInfoListenerEnabled) {
            listenerByName.put(statusInfoListenerName, Arrays.asList(statusInfoListener));
        }

        // Register with PoppyDB for optimizations if connected
        registerWithPoppyDB();

        // DualChannelMessaging (#265): ensure the DM collection (incl. its TTL index) exists
        // before wiring up its change stream / dispatcher. Done here (not in init()) because
        // setSenderId() can change getSenderId()/getDMCollectionName() between init() and start().
        morphium.ensureIndicesFor(Msg.class, getDMCollectionName());
        log.warn("DualChannelMessaging (beta, #265) starting on queue '{}': ALL messaging "
                + "participants on this queue must run DualChannelMessaging for DM/answer "
                + "delivery to work - a legacy StandardMessaging node's answers to THIS "
                + "instance's requests will time out (see class javadoc).", getCollectionName());

        dmDispatcherRunning = true;
        dmDispatcherThread = Thread.ofPlatform().name("msg-dm-" + id).start(this::dmDispatcherLoop);

        if (useChangeStream) {
            log.info("Changestream init");
            initChangeStreams();
            initDmChangeStream();
            // Signal that messaging is now ready (change streams are initialized)
            ready = true;
            readyLatch.countDown();
            log.info("Messaging {} is now ready", id);
        } else {
            // No change streams, ready immediately
            ready = true;
            readyLatch.countDown();
            log.info("Messaging {} is now ready (no change streams)", id);
        }

        // always run this find in addition to changestream
        try {
            AtomicLong lastRun = new AtomicLong(System.currentTimeMillis());
            // Safety-net poll runs every messagingFallbackPollInterval regardless of stream
            // health (see the poll conditions below for why). Stream liveness - the watch loop
            // receives a server reply at least every maxTimeMS (empty batch heartbeat) - is
            // used to poll IMMEDIATELY when a stream falls silent, instead of on the timer.
            final AtomicLong lastFallbackPoll = new AtomicLong(0);
            final AtomicBoolean streamsWereLive = new AtomicBoolean(false);
            // DM-lane fallback-poll bookkeeping (mirrors the main lane's, separate state) and the
            // registry-gated orphan DM collection sweep (#265) - both run on this same tick,
            // deliberately no dedicated cron thread.
            final AtomicLong lastDmFallbackPoll = new AtomicLong(0);
            final AtomicBoolean dmStreamWasLive = new AtomicBoolean(false);
            final AtomicLong lastDmOrphanSweep = new AtomicLong(0);
            decouplePool.scheduleWithFixedDelay(() -> {

                try {
                    // Liveness-check first — see checkMainThreadAlive() for the failure mode.
                    checkMainThreadAlive();
                    // Cleanup old message tracking entries to prevent unbounded memory growth
                    long cleanupTime = System.currentTimeMillis();
                    locallyProcessedMessageIds.entrySet().removeIf(entry ->
                                              cleanupTime - entry.getValue() > MESSAGE_TRACKING_RETENTION_MS);

                    // Poll when:
                    // 1. requestPoll > 0 (lock deleted / watch re-established, messages likely available)
                    // 2. change streams disabled (always poll)
                    // 3. fallback: every messagingFallbackPollInterval - ALWAYS, even while the
                    //    streams are live: messages can (re-)appear without any matching stream
                    //    event (e.g. requeueing by clearing processedBy via a plain DB update)
                    //    and must be found before their TTL expires. Liveness only ADDS urgency:
                    //    a stream falling silent is polled immediately instead of on the timer.
                    boolean shouldFallbackPoll = false;

                    if (useChangeStream) {
                        boolean live = changeStreamsLive();
                        boolean justTurnedSuspect = streamsWereLive.getAndSet(live) && !live;
                        long now = System.currentTimeMillis();

                        if (justTurnedSuspect
                                || now - lastFallbackPoll.get() >= settings.getMessagingFallbackPollInterval()) {
                            lastFallbackPoll.set(now);
                            shouldFallbackPoll = true;
                        }
                    }

                    if (requestPoll.get() > 0 || !useChangeStream || shouldFallbackPoll) {
                        lastRun.set(System.currentTimeMillis());
                        morphium.inc(StatisticKeys.PULL);
                        StatisticValue sk = morphium.getStats().get(StatisticKeys.PULLSKIP);
                        sk.set(sk.get() + requestPoll.get());
                        requestPoll.set(0);
                        boolean foundBacklog = findMessages();
                        // Watchdog: if the poll picks up unprocessed messages but the change stream
                        // has been silent for too long, the cursor has fallen behind — restart it.
                        // Threshold = 2 fallback intervals before declaring a stall, which avoids
                        // false positives from slow networks.
                        if (foundBacklog && useChangeStream) {
                            restartMainCsIfStalled(2L * settings.getMessagingFallbackPollInterval());
                        }
                    } else {
                        morphium.inc(StatisticKeys.SKIPPED_MSG_UPDATES);
                    }

                    // --- DM lane fallback poll (#265): same three trigger conditions as the main
                    // lane, applied to the DM collection/cursor instead. ---
                    boolean shouldDmFallbackPoll = false;

                    if (useChangeStream) {
                        boolean dmLive = dmChangeStreamLive();
                        boolean dmJustTurnedSuspect = dmStreamWasLive.getAndSet(dmLive) && !dmLive;
                        long now = System.currentTimeMillis();

                        if (dmJustTurnedSuspect
                                || now - lastDmFallbackPoll.get() >= settings.getMessagingFallbackPollInterval()) {
                            lastDmFallbackPoll.set(now);
                            shouldDmFallbackPoll = true;
                        }
                    }

                    if (requestDmPoll.get() > 0 || !useChangeStream || shouldDmFallbackPoll) {
                        requestDmPoll.set(0);
                        findDmMessages();
                    }

                    // --- Orphan DM collection sweep (#265): registry-gated, drops dead+empty
                    // per-recipient DM collections belonging to former participants of this
                    // queue. Never touches a collection with documents in it (a live participant's
                    // collection could transiently be empty between messages - dropping it would
                    // lose its TTL index). ---
                    if (networkRegistry != null && settings.isMessagingDmCleanupOrphansOnStartup()) {
                        long now = System.currentTimeMillis();

                        if (now - lastDmOrphanSweep.get() >= settings.getMessagingRegistryUpdateInterval() * 1000L) {
                            lastDmOrphanSweep.set(now);
                            sweepOrphanDmCollections();
                        }
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
                        log.debug("PROCESSING: {} Added {} to idsInProgress (from queue)", id, prEl.getId());
                    } else {
                        log.debug("PROCESSING: {} Polled {} (already in idsInProgress from changestream)", id, prEl.getId());
                    }
                }

                final ProcessingQueueElement finalPrEl = prEl;
                // Distinguishes "never dequeued" from "runnable stuck in the thread pool" from
                // "runnable ran and bailed" in the decision trace (see the BasicJMSTests flaky:
                // an answer was queued for processing and then nothing happened for 30s).
                traceDecision(finalPrEl.getId(), null, "processing: dequeued, submitting runnable");
                Runnable r = () -> {
                    boolean wasProcessed = false;
                    Msg msg = null;
                    try {
                        traceDecision(finalPrEl.getId(), null, "processing: runnable started");

                        if (!running || morphium == null || morphium.getDriver() == null) {
                            traceDecision(finalPrEl.getId(), null, "processing: bailing - messaging not running / driver gone");
                            return;
                        }

                        // CRITICAL: Use PRIMARY read preference to avoid stale reads from replicas
                        // With NEAREST, replica lag could cause us to see old processedBy values
                        // which would cause message processing to be incorrectly skipped
                        var q = morphium.createQueryFor(Msg.class)
                                .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY).f("_id").eq(finalPrEl.getId());
                        q.setCollectionName(getCollectionName());
                        msg = q.get();

                        if (msg == null) {
                            traceDecision(finalPrEl.getId(), null, "processing: reread returned null - message gone from collection");
                            return;
                        }

                        // do not process if no listener registered for this message
                        if (!msg.isAnswer() && !getListenerNames().containsKey(msg.getTopic())) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: no listener for topic '" + msg.getTopic() + "', skipped");
                            return;
                        }

                        // Never receive messages sent by myself by default.
                        // sendMessageToSelf() uses sender="self" and explicit recipient, so it still works.
                        if (msg.getSender() != null && msg.getSender().equals(id)) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: sent by myself (sender==me), skipped");
                            return;
                        }

                        // exclusive message already processed
                        if (msg.isExclusive() && msg.getProcessedBy() != null && msg.getProcessedBy().size() != 0) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: exclusive already processed by " + msg.getProcessedBy() + ", skipped");
                            return;
                        }

                        // I did already process this message
                        // For all drivers, check the stale copy first (fast path)
                        if (msg.getProcessedBy() != null && msg.getProcessedBy().contains(id)) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: already processed by me, skipped");
                            return;
                        }

                        // recipient specified, but i am not it
                        if (msg.getRecipients() != null && !msg.getRecipients().contains(id)) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: not a recipient (recipients=" + msg.getRecipients() + "), skipped");
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
                                traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: answer matched waiting request, delivered");

                                // Deliver BEFORE persisting the processed_by mark: that write is
                                // majority-acked on real MongoDB (easily 10ms+ per call) and the blocked
                                // sendAndAwait* caller must not pay for it. The local object is marked
                                // first so the delivered answer carries consistent metadata; redelivery
                                // during the gap cannot happen because this block runs inside the
                                // processing runnable and the id stays in idsInProgress (excluded by
                                // both the poll query and the change stream duplicate check) until the
                                // write has landed.
                                if (!msg.getProcessedBy().contains(id)) {
                                    msg.getProcessedBy().add(id);
                                }

                                if (!answersForMessage.contains(msg)) {
                                    answersForMessage.add(msg);
                                }

                                persistProcessedByMark(msg);
                                checkDeleteAfterProcessing(msg);
                                return;
                            }

                            final CallbackRequest cbr = waitingForCallbacks.get(msg.getInAnswerTo());
                            final Msg theMessage = msg;

                            if (cbr != null) {
                                traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: answer matched async callback, delivered");
                                AsyncMessageCallback cb = cbr.callback;
                                Runnable cbRunnable = () -> {
                                    cb.incomingMessage(theMessage);
                                };
                                // Dispatch BEFORE persisting the processed_by mark - same reasoning as
                                // the waiter path above: the callback must not wait for the write, and
                                // idsInProgress shields against redelivery until the write has landed.
                                if (!theMessage.getProcessedBy().contains(id)) {
                                    theMessage.getProcessedBy().add(id);
                                }

                                queueOrRun(cbRunnable);
                                persistProcessedByMark(theMessage);

                                if (cbr.theMessage.isExclusive()) {
                                    waitingForCallbacks.remove(msg.getInAnswerTo());
                                }

                                checkDeleteAfterProcessing(msg);
                                return;
                            }

                            // neither a waiter nor a callback: the request may have timed out
                            // already - or this is the exact silent drop the trace exists for
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: answer matched NO waiter/callback, falling through to topic listeners");
                        }

                        if (!getListenerNames().containsKey(msg.getTopic())) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: no listener for topic '" + msg.getTopic() + "', dropped");
                            return;
                        }

                        // really do handle message
                        if (msg.isExclusive()) {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: handling exclusively (lockAndProcess)");
                            lockAndProcess(msg);
                            wasProcessed = true;
                        } else {
                            traceDecision(msg.getMsgId(), msg.getInAnswerTo(), "processing: handling (processMessage)");
                            processMessage(msg);
                            wasProcessed = true;
                        }
                    } finally {
                        synchronized (processing) {
                            // Always remove from idsInProgress when done processing
                            // The database processed_by field handles duplicate prevention
                            idsInProgress.remove(finalPrEl.getId());
                            log.debug("PROCESSING: {} Removed {} from idsInProgress", id, finalPrEl.getId());
                        }
                    }
                };
                queueOrRun(r);
            } catch (Throwable t) {
                // Catch Throwable (not just Exception) so that an Error like OutOfMemoryError
                // does not silently kill the main messaging thread. A dead main thread leaves
                // the change stream cursor running, listeners attached, and the processing
                // queue filling up — but no one drains it, so the whole messaging instance
                // appears alive while delivering nothing. Logging here at least makes the
                // failure visible; the next allocation attempt may still re-throw, but the
                // operator can see what is happening instead of staring at a silent log.
                if (running) {
                    log.error("Unhandled throwable in messaging main loop for '{}' — keeping thread alive",
                            getCollectionName(), t);
                }
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
                return q.q().f(Msg.Fields.sender).ne(id).f(processedByFieldName).ne(id).f(Msg.Fields.inAnswerTo)
                       .in(waitingForAnswers.keySet()).limit(windowSize).idList();
            }

            // Skip messages already being processed locally
            // No need to query lock collection - lockMessage() handles already-locked cases atomically
            var idsToIgnore = new ArrayList<MorphiumId>();

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
            var q1 = q.q().f(Msg.Fields.exclusive).eq(true).f(processedByFieldName + ".0").notExists();
            // q2: non-exclusive messages, cannot be locked, not processed by me yet
            var q2 = q.q().f(Msg.Fields.exclusive).ne(true).f(processedByFieldName).ne(id);
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

    /**
     * @return true if at least one unprocessed message was found by the poll. The caller uses this
     * as a signal that there is real backlog (used by the change stream watchdog).
     */
    private boolean findMessages() {
        if (!running) {
            return false;
        }

        // log.debug("getting messages...");
        List<ProcessingQueueElement> messages = getMessagesForProcessing();

        if (messages == null) {
            return false;
        }

        if (messages.size() == 0) {
            return false;
        }

        for (ProcessingQueueElement el : messages) {
            synchronized (processing) {
                if (!processing.contains(el) && !idsInProgress.contains(el.getId())) {
                    processing.add(el);
                    // NOTE: We do NOT add to idsInProgress here
                    // It will be added when the processing thread pulls it from the queue
                    // This prevents messages from getting stuck in idsInProgress if never processed
                    traceDecision(el.getId(), null, "poll: queued for processing");
                } else {
                    traceDecision(el.getId(), null, "poll: skipped (already queued or in progress)");
                }
            }
        }
        return true;
    }

    @SuppressWarnings("CommentedOutCode")
    private void lockAndProcess(Msg obj) {
        if (lockMessage(obj, id, obj.getDeleteAt())) {
            // Messages can be queued for processing before the exclusive lock exists.
            // If another instance already processed and unlocked, a stale queued message
            // could otherwise be processed again. Re-fetch to ensure processed_by state is current.
            // CRITICAL: Use PRIMARY read preference to avoid reading stale data from secondary replicas
            Msg fresh = null;
            try {
                fresh = morphium.createQueryFor(Msg.class, getCollectionName())
                        .setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY)
                        .f("_id").eq(obj.getMsgId())
                        .get();
            } catch (Exception ignored) {
            }

            if (fresh == null) {
                // Message was deleted - release lock for cleanup
                unlockForRetry(obj);
                return;
            }

            if (fresh.getProcessedBy() != null && !fresh.getProcessedBy().isEmpty()) {
                // Already processed by someone else - release our lock
                unlockForRetry(fresh);
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
        } else if (m.isTimingOut() && m.getTtl() > 0) {
            // Message has TTL → lock expires after TTL * 2
            lck.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl() * 2));
        } else {
            // Message has no timeout → lock should also not expire quickly
            // Use 7 days as fallback to prevent stuck locks if job crashes
            lck.setDeleteAt(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000L));
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
            locallyMarked = locallyProcessedMessageIds.putIfAbsent(msg.getMsgId(), System.currentTimeMillis()) == null;
            if (!locallyMarked) {
                return;
            }
        }

        // outdated message
        if (msg.isTimingOut() && msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
            // Delete outdated msg!
            log.debug(getSenderId() + ": Found outdated message - deleting it!");
            morphium.delete(msg, getCollectionName());
            unlockForRetry(msg);
            return;
        }

        if (msg.isExclusive() && msg.getProcessedBy().size() > 0) {
            // Already processed by someone else - they have the lock, not us
            return;
        }

        boolean alreadyUpdatedProcessedBy = false;
        // Duplicate execution is prevented by queue/idsInProgress deduplication and the processed_by checks.
        // Avoid extra database roundtrips here (especially for InMemoryDriver), as they severely impact throughput.
        if (msg.getProcessedBy().contains(id)) {
            // We already processed this message
            return;
        }

        if (listenerByName.isEmpty()) {
            // message cannot be processed, as no listener is defined and message is no
            // answer.
            // if (log.isDebugEnabled()) {
            //     log.debug("Not further processing - no listener for non answer message");
            // }

            // removeProcessingFor(msg);
            unlockForRetry(msg);
            // Allow retry if listener is added later
            if (locallyMarked) {
                locallyProcessedMessageIds.remove(msg.getMsgId());
            }
            // log.info("not processing");
            return;
        }

        // Runnable r = ()->{
        boolean wasProcessed = false;
        boolean wasRejected = false;
        boolean markedForExclusiveOnly = false;
        List<MessageRejectedException> rejections = new ArrayList<>();
        List<MessageListener> lst = new ArrayList<>();

        if (listenerByName.get(msg.getTopic()) != null) {
            lst.addAll(listenerByName.get(msg.getTopic()));
        }

        for (MessageListener l : lst) {
            try {
                if (pauseMessages.containsKey(msg.getTopic())) {
                    // paused - do not process, release lock for retry later
                    // log.warn("Received paused message?!?!? "+msg1.getMsgId());
                    // processing.remove(msg.getMsgId());
                    // removeProcessingFor(msg);
                    wasProcessed = false;
                    unlockForRetry(msg);
                    // Allow retry when topic is unpaused
                    if (locallyMarked) {
                        locallyProcessedMessageIds.remove(msg.getMsgId());
                    }
                    break;
                }

                // Exclusive messages must set processed_by BEFORE onMessage: exactly-once must not
                // rely on the MsgLock alone. If the lock is lost mid-processing (TTL, cleanup, failover)
                // and the message is re-fetched via the poll path, a second instance would re-lock,
                // see an empty processed_by and process again.
                if ((l.markAsProcessedBeforeExec() || msg.isExclusive()) && !alreadyUpdatedProcessedBy) {
                    boolean updated = updateProcessedBy(msg);
                    if (!updated) {
                        // processed_by update failed - don't call listener, allow retry
                        log.error("Failed to update processed_by before exec for message {} - allowing retry", msg.getMsgId());
                        if (locallyMarked) {
                            locallyProcessedMessageIds.remove(msg.getMsgId());
                        }
                        return;
                    }
                    alreadyUpdatedProcessedBy = true;
                    markedForExclusiveOnly = !l.markAsProcessedBeforeExec();
                }

                // Call listener
                Msg answer = l.onMessage(DualChannelMessaging.this, msg);
                wasProcessed = true;

                if (autoAnswer && answer == null) {
                    answer = new Msg(msg.getTopic(), "received", "");
                }

                if (answer != null) {
                    msg.sendAnswer(DualChannelMessaging.this, answer);

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

                // rollback the pre-exec mark that was only set because the message is exclusive,
                // otherwise the message could never be retried
                if (markedForExclusiveOnly) {
                    removeProcessedBy(msg);
                    alreadyUpdatedProcessedBy = false;
                    markedForExclusiveOnly = false;
                }
            } catch (Exception e) {
                log.error(id + ": listener Processing failed", e);

                // same rollback as for rejections - keep retry semantics for exclusive messages
                if (markedForExclusiveOnly) {
                    removeProcessedBy(msg);
                    alreadyUpdatedProcessedBy = false;
                    markedForExclusiveOnly = false;
                }
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
            boolean updated = updateProcessedBy(msg);
            if (!updated) {
                // CRITICAL: Don't release lock if processed_by update failed
                // This prevents another receiver from processing the message again
                // The lock will expire via TTL and the message will be retried
                log.error("Failed to update processed_by for message {} - keeping lock to prevent duplicate", msg.getMsgId());
                return;
            }
        }

        if (!wasRejected && wasProcessed) {
            checkDeleteAfterProcessing(msg);
        }

        unlockIfExclusive(msg);
    }

    /**
     * Release lock for exclusive message to allow retry by another instance.
     * Use this when message was NOT successfully processed (outdated, no listeners, paused, etc.)
     */
    private void unlockForRetry(Msg msg) {
        if (msg.isExclusive()) {
            deleteLock(msg.getMsgId());
        }
    }

    /**
     * Called after successful exclusive message processing.
     * Delete the lock after processing is complete.
     * This is safe because processed_by has already been updated before this is called,
     * so even if another instance reads the message before the lock is deleted,
     * they'll see our ID in processed_by and skip re-processing.
     */
    private void unlockIfExclusive(Msg msg) {
        if (msg.isExclusive()) {
            deleteLock(msg.getMsgId());
        }
    }

    private void deleteLock(MorphiumId msgId) {
        morphium.createQueryFor(MsgLock.class).setCollectionName(getLockCollectionName()).f("_id").eq(msgId)
                .f("lock_id").eq(id).remove();
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
        Map<String, Object> set = Doc.of(processedByFieldName, id);
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

    /**
     * DB-only companion to updateProcessedBy for the answer fast path: the local Msg object has
     * already been marked (so the delivered answer carries consistent metadata) and only the
     * $addToSet write remains. Deliberately no local-contains shortcut - the local list was just
     * mutated, the write must still be attempted. nModified=0 means someone else marked the
     * message or it was already deleted (deleteAfterProcessing race); both are fine for answers,
     * $addToSet is idempotent either way.
     */
    private void persistProcessedByMark(Msg msg) {
        if (msg == null || !running || morphium == null || morphium.getDriver() == null || morphium.getConfig() == null) {
            return;
        }

        Object queryId = msg.getMsgId();
        if (queryId instanceof MorphiumId) {
            queryId = new org.bson.types.ObjectId(((MorphiumId) queryId).getBytes());
        }
        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName());
        idq.f("_id").eq(queryId);
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
            cmd.addUpdate(idq.toQueryObject(), Doc.of("$addToSet", Doc.of(processedByFieldName, id)),
                          null, false, false, null, null, null);
            cmd.execute();
            cmd.releaseConnection();
            cmd = null;
        } catch (MorphiumDriverException e) {
            log.error("Error persisting processed_by mark for answer " + msg.getMsgId(), e);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    /**
     * Rollback for updateProcessedBy: remove our id from processed_by ($pull) so the message
     * can be retried. Used when an exclusive message was marked before exec (see processMessage)
     * but the listener rejected or failed.
     */
    private void removeProcessedBy(Msg msg) {
        if (msg == null) {
            return;
        }

        if (!running || morphium == null || morphium.getDriver() == null || morphium.getConfig() == null) {
            return;
        }

        Object queryId = msg.getMsgId();
        if (queryId instanceof MorphiumId) {
            queryId = new org.bson.types.ObjectId(((MorphiumId) queryId).getBytes());
        }
        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName());
        idq.f("_id").eq(queryId);
        Map<String, Object> qobj = idq.toQueryObject();
        Map<String, Object> update = Doc.of("$pull", Doc.of(processedByFieldName, id));
        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(Msg.class)));
            cmd.setColl(getCollectionName()).setDb(morphium.getDatabase());
            cmd.addUpdate(qobj, update, null, false, false, null, null, null);
            cmd.execute();
            cmd.releaseConnection();
            cmd = null;
            msg.getProcessedBy().remove(id);
        } catch (MorphiumDriverException e) {
            log.error("Error removing processed_by - message might not be retried!", e);
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
            } catch (Throwable t) {
                // If thread pool execution fails (e.g., executor shutdown),
                // run synchronously to ensure cleanup (idsInProgress removal) happens
                log.warn("Thread pool execution failed, running synchronously: {}", t.getMessage());
                try {
                    r.run();
                } catch (Throwable inner) {
                    log.error("Error running task synchronously after thread pool failure", inner);
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
    public DualChannelMessaging setSenderId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public int getPause() {
        return pause;
    }

    @Override
    public DualChannelMessaging setPause(int pause) {
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

    /**
     * Returns true if messaging is fully initialized and ready to process messages.
     * This includes having change stream subscriptions active (if using change streams).
     */
    public boolean isReady() {
        return ready;
    }

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
    public boolean waitForReady(long timeout, TimeUnit unit) throws InterruptedException {
        return readyLatch.await(timeout, unit);
    }

    @Override
    public void close() {
        terminate();
    }

    @Override
    public void terminate() {
        log.info("Terminate messaging");
        // Unregister from PoppyDB before terminating
        unregisterFromPoppyDB();
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

        // DualChannelMessaging (#265): stop the DM monitor + dispatcher and drop this instance's
        // own per-recipient DM collection. Other instances' DM collections are left untouched
        // here - that's what the registry-gated orphan sweep is for (see sweepOrphanDmCollections).
        if (dmMonitor != null) {
            dmMonitor.terminate();
        }

        dmDispatcherRunning = false;

        if (dmDispatcherThread != null) {
            dmDispatcherThread.interrupt();

            try {
                dmDispatcherThread.join(2000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        dmProcessing.clear();
        dmIdsInProgress.clear();
        requestDmPoll.set(0);

        try {
            morphium.dropCollection(Msg.class, getDMCollectionName(), null);
        } catch (Exception e) {
            log.warn("Error dropping own DM collection '{}' on terminate: {}", getDMCollectionName(), e.getMessage());
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
        // Don't send messages if messaging has been terminated
        if (!running) {
            log.debug("Messaging terminated - not sending message: {}", m.getTopic());
            return;
        }
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

        // apply the configured default TTL (Msg.preStore would fall back to the hardcoded 30s)
        if (m.isTimingOut() && m.getTtl() <= 0) {
            m.setTtl(settings.getMessagingDefaultTtl());
        }
        m.setSenderHost(hostname);
        try {
            // DualChannelMessaging send-side routing (#265): directed messages (recipients set -
            // requests as well as answers, via Msg#sendAnswer/#setRecipient) go into EACH
            // recipient's own per-recipient DM collection instead of the shared main collection.
            // Broadcasts/topic messages (no recipients) keep using the main collection, watched
            // by the unmodified main-lane change stream - bit-identical to SingleCollectionMessaging.
            if (m.getRecipients() != null && !m.getRecipients().isEmpty()) {
                for (String recipient : m.getRecipients()) {
                    String dmColl = getDMCollectionName(recipient);
                    morphium.insert(m, dmColl, cb);
                    scheduleTimeoutDeletionIfNeeded(m, dmColl);
                }
            } else {
                morphium.insert(m, getCollectionName(), cb);
                scheduleTimeoutDeletionIfNeeded(m, getCollectionName());
            }
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
        // Recipient is always "self" (== id) here, so this goes straight into my own DM collection.
        morphium.insert(m, getDMCollectionName(), cb);
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
     * @param timeoutInMs - milliseconds to wait until listener is removed
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

    /**
     * Failure-path-only diagnostics for the recurring answer-timeout flakies (BasicJMSTests
     * et al., 2026-07-21): query the collection state once and name the failing stage -
     * request never processed (delivery/processing), processed without an answer (answer
     * never sent), or answer stored but not delivered back (answer delivery).
     */
    private void logAnswerTimeoutDiagnostics(Msg theMessage, long timeoutInMs) {
        try {
            Msg orig = morphium.createQueryFor(Msg.class, getCollectionName())
                .f("_id").eq(theMessage.getMsgId()).get();
            long answers = morphium.createQueryFor(Msg.class, getCollectionName())
                .f(Msg.Fields.inAnswerTo).eq(theMessage.getMsgId()).countAll();
            String verdict;

            if (answers > 0) {
                verdict = "answer(s) stored but not delivered back to this instance (answer delivery failed)";
            } else if (orig == null) {
                verdict = "request gone (deleted/expired) and no answer stored";
            } else if (orig.getProcessedBy() == null || orig.getProcessedBy().isEmpty()) {
                verdict = "request still unprocessed (delivery to/processing by the consumer failed)";
            } else {
                verdict = "request processed by " + orig.getProcessedBy() + " but no answer stored (answer never sent)";
            }

            log.error("answer timeout diagnostics for {}/{} after {}ms (instance {}): request={}, answers stored={} -> {}",
                theMessage.getTopic(), theMessage.getMsgId(), timeoutInMs, getSenderId(),
                orig == null ? "GONE" : "present, processedBy=" + orig.getProcessedBy(), answers, verdict);
            // "answers stored=0" at exactly t=timeout can be a TTL artifact (answer TTL often
            // equals the await timeout) - the decision trace shows what THIS instance actually
            // did with the request and any answer to it, including the silent skip paths.
            List<String> decisions = getProcessingDecisions(theMessage.getMsgId());
            log.error("processing decision trace for {} ({} entries): {}",
                theMessage.getMsgId(), decisions.size(), decisions.isEmpty() ? "NONE - no event or poll ever saw this id" : String.join(" | ", decisions));
        } catch (Exception e) {
            log.error("answer timeout diagnostics failed", e);
        }
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

            if (null == firstAnswer) {
                logAnswerTimeoutDiagnostics(theMessage, timeoutInMs);

                if (throwExceptionOnTimeout) {
                    throw new MessageTimeoutException("Did not receive answer for message " + theMessage.getTopic() + "/"
                                                      + requestMsgId + " in time (" + timeoutInMs + "ms)");
                }
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
                    logAnswerTimeoutDiagnostics(theMessage, timeout);
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

    /**
     * @deprecated use {@link #setWindowSize(int)} instead (windowSize 1 corresponds to processMultiple == false)
     */
    @Deprecated(since = "6.3", forRemoval = true)
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

    // MessageTimeoutException, SystemShutdownException, ProcessingQueueElement and
    // AsyncMessageCallback are intentionally NOT duplicated here - they are imported from
    // SingleCollectionMessaging (see imports above). AsyncMessageCallback in particular is
    // referenced by type in the MorphiumMessaging interface signature
    // (sendAndAwaitAsync(..., SingleCollectionMessaging.AsyncMessageCallback)), so a locally
    // cloned type would not satisfy the override.

    @Override
    public <T extends Msg> String getLockCollectionName(T topic) {
        return getLockCollectionName();
    }

    @Override
    public String getLockCollectionName(String topic) {
        return getLockCollectionName();
    }

    /**
     * @return the name of this instance's own per-recipient DM/answer collection.
     */
    public String getDMCollectionName() {
        return getDMCollectionName(getSenderId());
    }

    /**
     * Per-recipient DM/answer collection name. Deliberately includes the queue prefix
     * ({@link #getCollectionName()}), unlike {@code MultiCollectionMessaging}'s {@code "dm_" +
     * camelCase(sender)}: this way multiple independent queues on the same database never share
     * a DM collection for the same sender id.
     */
    @Override
    public String getDMCollectionName(String sender) {
        return getCollectionName() + "_dm_" + morphium.getARHelper().createCamelCase(sender, false);

    }
}
