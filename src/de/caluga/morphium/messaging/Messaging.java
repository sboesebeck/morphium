package de.caluga.morphium.messaging;

import de.caluga.morphium.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.EmptyIterator;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:48
 * <p/>
 * Messaging implements a simple, threadsafe and messaging api. Used for cache synchronization.
 * Msg can have several modes:
 * - LockedBy set to ALL (Exclusive Messages): every listener may process it (in parallel), means 1->n. e.g. Cache sync
 * - LockedBy null (non exclusive messages): only one listener at a time
 * - Message listeners may return a Message as answer. Or throw a MessageRejectedException.c
 */
@SuppressWarnings({"ConstantConditions", "unchecked", "UnusedDeclaration"})
public class Messaging extends Thread implements ShutdownListener {
    private static final Logger log = LoggerFactory.getLogger(Messaging.class);

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

    private ThreadPoolExecutor threadPool;
    private final ScheduledThreadPoolExecutor decouplePool;

    private boolean multithreadded;
    private int windowSize;
    private boolean useChangeStream;
    private ChangeStreamMonitor changeStreamMonitor;

    private final Map<MorphiumId, Msg> waitingForAnswers = new ConcurrentHashMap<>();
    private final Map<MorphiumId, Msg> waitingForMessages = new ConcurrentHashMap<>();

    private final List<MorphiumId> processing = new Vector<>();

    private final AtomicInteger skipped = new AtomicInteger(0);

    /**
     * attaches to the default queue named "msg"
     *
     * @param m               - morphium
     * @param pause           - pause between checks
     * @param processMultiple - process multiple messages at once, if false, only ony by one
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

    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize, boolean useChangeStream, ReceiveAnswers recieveAnswers) {
        setMultithreadded(multithreadded);
        setWindowSize(windowSize);
        setUseChangeStream(useChangeStream);
        setReceiveAnswers(recieveAnswers);
        setQueueName(queueName);
        setPause(pause);
        setProcessMultiple(processMultiple);
        morphium = m;


        if (multithreadded) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>() {
                @Override
                public boolean offer(Runnable e) {
                    /*
                     * Offer it to the queue if there is 0 items already queued, else
                     * return false so the TPE will add another thread. If we return false
                     * and max threads have been reached then the RejectedExecutionHandler
                     * will be called which will do the put into the queue.
                     */
//                    if (size() == 0) {
//                        return super.offer(e);
//                    } else {
//                        return false;
//                    }
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
                    morphium.getConfig().getThreadPoolMessagingKeepAliveTime(), TimeUnit.MILLISECONDS,
                    queue);
            threadPool.setRejectedExecutionHandler((r, executor) -> {
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
            //noinspection unused,unused
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

        decouplePool = new ScheduledThreadPoolExecutor(1);
        //noinspection unused,unused
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
                //swallow
            }
        }
        if (hostname == null) {
            hostname = "unknown host";
        }
        m.ensureIndicesFor(Msg.class, getCollectionName());
        //        try {
        //            m.ensureIndex(Msg.class, Msg.Fields.lockedBy, Msg.Fields.timestamp);
        //            m.ensureIndex(Msg.class, Msg.Fields.lockedBy, Msg.Fields.processedBy);
        //            m.ensureIndex(Msg.class, Msg.Fields.timestamp);
        //        } catch (Exception e) {
        //            log.error("Could not ensure indices", e);
        //        }

        listeners = new CopyOnWriteArrayList<>();
        listenerByName = new HashMap<>();
    }

    public long getPendingMessagesCount() {
        Query<Msg> q1 = morphium.createQueryFor(Msg.class, getCollectionName());

        Query<Msg> or1 = q1.q().f(Msg.Fields.sender).ne(id)
                .f(Msg.Fields.lockedBy).in(Arrays.asList(null, id))
                .f(Msg.Fields.processedBy).eq(null);
        Query<Msg> or2 = q1.q().f(Msg.Fields.sender).ne(id)
                .f(Msg.Fields.lockedBy).eq("ALL")
                .f(Msg.Fields.processedBy).ne(id);
        q1.or(or1, or2);
        long ret = q1.countAll();
        return ret;
    }

    public void removeMessage(Msg m) {
        morphium.delete(m, getCollectionName());
    }

    @Override
    public void run() {
        setName("Msg " + id);
//        if (log.isDebugEnabled()) {
//            log.debug("Messaging " + id + " started");
//        }


        if (useChangeStream) {
//            log.debug("Before running the changestream monitor - check of already existing messages");
            try {
                findAndProcessPendingMessages(null);
                if (multithreadded) {
                    //wait for preprocessing to finish
                    while (threadPool != null && threadPool.getActiveCount() > 0) {
                        Thread.yield();
                    }
                }
            } catch (Exception e) {
                log.error("Error processing existing messages in queue", e);
            }
            log.debug("init Messaging  using changestream monitor");
            //changeStreamMonitor = new changeStream(morphium, getCollectionName(), false);

            //pipeline for reducing incoming traffic
            List<Map<String, Object>> pipeline = new ArrayList<>();
            Map<String, Object> match = new LinkedHashMap<>();
            Map<String, Object> in = new LinkedHashMap<>();
            in.put("$in", Arrays.asList("insert", "update"));
            match.put("operationType", in);
            pipeline.add(Utils.getMap("$match", match));
            changeStreamMonitor = new ChangeStreamMonitor(morphium, getCollectionName(), true, pipeline);
            changeStreamMonitor.addListener(evt -> {
//                    log.debug("incoming message via changeStream");
                try {
                    if (evt == null || evt.getOperationType() == null) return running;

                    if (evt.getOperationType().equals("insert")) {
                        //insert => new Message
//                        log.info(id+": incoming insert "+ Utils.toJsonString(evt.getFullDocument()));
                        Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());
                        if (obj.getInAnswerTo() != null) {
                            switch (receiveAnswers) {
                                case ALL:
                                    break;
                                case NONE:
                                    if (waitingForMessages.containsKey(obj.getInAnswerTo())) {
                                        break;
                                    }
                                    return running;
                                case ONLY_MINE:
                                    if (waitingForMessages.containsKey(obj.getInAnswerTo())) {
                                        break;
                                    }
                                    if (!(obj.getRecipients() != null && obj.getRecipients().contains(id))) {
                                        return running;
                                    }
                            }
                        }
                        if (obj.getInAnswerTo() != null && waitingForMessages.containsKey(obj.getInAnswerTo())) {
                            if (log.isDebugEnabled())
                                log.debug("processing answer " + obj.getMsgId() + " in answer to " + obj.getInAnswerTo());
                            List<Msg> lst = new ArrayList<>();
                            lst.add(obj);
                            try {
                                processMessages(lst);
                            } catch (Exception e) {
                                log.error("Error during message processing ", e);
                            }
                            return running;
                        }
                        if (listenerByName.get(obj.getName()) == null && listeners.size() == 0) {
                            //ignoring incoming message, we do not have listener for
                            return running;
                        }
                        if (pauseMessages.containsKey(obj.getName())) {
                            skipped.incrementAndGet();
                            return running;
                        }
                        if (obj.getSender().equals(id) || obj.getProcessedBy().contains(id) || (obj.getRecipients() != null && !obj.getRecipients().contains(id))) {
                            //ignoring my own messages
                            return running;
                        }
                        //do not process messages, that are exclusive, but already processed or not for me / all
                        if (obj.isExclusive() && obj.getLockedBy() == null && (obj.getRecipients() == null || obj.getRecipients().contains(id)) && obj.getProcessedBy().size() == 0) {
                            // locking
                            lockAndProcess(obj);

                        } else if (!obj.isExclusive() || (obj.getRecipients() != null && obj.getRecipients().contains(id))) {
                            //I need process this new message... it is either for all or for me directly
                            if (processing.contains(obj.getMsgId())) {
                                return running;
                            }
                            //                        obj = morphium.reread(obj);
                            //                        if (obj.getReceivedBy() != null && obj.getReceivedBy().contains(id)) {
                            //                            //already got this message - is still being processed it seems
                            //                            return;
                            //                        }
                            List<Msg> lst = new ArrayList<>();
                            lst.add(obj);

                            try {
                                processMessages(lst);
                            } catch (Exception e) {
                                log.error("Error during message processing ", e);
                            }
                        } else {
                            //log.debug("Message is not for me");
                        }

                    } else if (evt.getOperationType().equals("update")) {
                        //dealing with updates... i could have "lost" a lock
                        //                        if (((Map<String,Object>)data.get("o")).get("$set")!=null){
                        //                            //there is a set-update
                        //                        }
//                        log.info(id+": incoming update");
//                        log.info(id+": updating: "+Utils.toJsonString(evt.getUpdatedFields()));
//                        log.info(id+": Removing: "+ Utils.toJsonString(evt.getRemovedFields()));

//                        if (evt.getUpdatedFields().size()==1&&evt.getUpdatedFields().containsKey("locked")){
//                            log.info(id+" refresh incoming");
//                        }
                        if (evt.getUpdatedFields() != null && evt.getUpdatedFields().containsKey("locked_by")) {
                            if (evt.getUpdatedFields().get("locked_by") != null) {
                                return running; //ignoring locking of messages
                            }
                            //lock was released
                        }
                        if (evt.getUpdatedFields() != null && evt.getUpdatedFields().containsKey("processed_by") && !evt.getUpdatedFields().containsKey("locked_by")) {
                            return running;
                        }

                        Msg obj = null;
                        if (evt.getDocumentKey() != null) {
                            obj = morphium.findById(Msg.class, evt.getDocumentKey(), getCollectionName());
                        }

                        if (obj == null) {
                            return running; //was deleted?
                        }
                        if (obj.getSender().equals(id) || obj.getProcessedBy().contains(id) || (obj.getRecipients() != null && !obj.getRecipients().contains(id))) {
                            //ignoring my own messages
                            return running;
                        }
//                        if (obj.getInAnswerTo() != null && waitingForMessages.containsKey(obj.getInAnswerTo())) {
//                            if (obj.isExclusive() && obj.getProcessedBy().size() == 0) {
//                                lockAndProcess(obj);
//                            } else {
//                                try {
//                                    processMessages(Arrays.asList(obj));
//                                } catch (Exception e) {
//                                    log.error("Error during message processing ", e);
//                                }
//                            }
//                        }
                        //ignore message, when no listener ist registered and message is not an answer, we are waiting for
                        if (listenerByName.get(obj.getName()) == null && listeners.size() == 0) {
                            if (obj.getInAnswerTo() == null || !waitingForMessages.containsKey(obj.getInAnswerTo()))
                                return running;
                        }
//                        if (obj.getInAnswerTo() != null) {
//                            switch (receiveAnswers) {
//                                case ALL:
//                                    break;
//                                case NONE:
//                                    return running;
//                                case ONLY_MINE:
//                                    if (waitingForMessages.containsKey(obj.getInAnswerTo())) {
//                                        break;
//                                    }
//                                    if (!(obj.getRecipients() != null && obj.getRecipients().contains(id))) {
//                                        return running;
//                                    }
//                            }
//                        }
                        if (pauseMessages.containsKey(obj.getName())) return running;
                        if (obj != null && obj.isExclusive()
                                && !obj.getSender().equals(id)
                                && (obj.getLockedBy() == null || obj.getLockedBy().equals(id))
                                && !obj.getProcessedBy().contains(id) && !pauseMessages.containsKey(obj.getName())
                                && (obj.getRecipients() == null || obj.getRecipients().contains(id))) {
//                                log.debug("Update of msg - trying to lock");
                            // locking
                            lockAndProcess(obj);
                        }
                        if (!obj.isExclusive() && !obj.getProcessedBy().contains(id)) {
                            processMessages(Arrays.asList(obj));
                        }
                    }


                } catch (Exception e) {
                    log.error("Error during event processing in changestream", e);
                }
                return running;
            });
            changeStreamMonitor.start();
        }
        findAndProcessMessages(processMultiple); //check for new msg on startup
        //always run this find in addtion to changestream
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
                log.error("Unhandled exception " + e.getMessage(), e);
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

    /**
     * pause processing for certain messages
     *
     * @param name
     */

    public void pauseProcessingOfMessagesNamed(String name) {
        pauseMessages.putIfAbsent(name, System.currentTimeMillis());
    }

    /**
     * unpause processing
     *
     * @param name
     * @return duration or null
     */
    public Long unpauseProcessingOfMessagesNamed(String name) {
        if (!pauseMessages.containsKey(name)) {
            return 0l;
        }

        Long ret = pauseMessages.remove(name);
        if (ret != null) {
            ret = System.currentTimeMillis() - ret;
        }
        skipped.incrementAndGet();
//        Runnable r = new Runnable() {
//            public void run() {
//                MorphiumIterator<Msg> messages = findMessages(name, processMultiple);
//                processMessages(messages);
//            }
//        };
//        int max = windowSize;
//        if (!multithreadded) {
//            max = 1;
//        }
//        while (decouplePool.getActiveCount() > max) {
//            Thread.yield();
//        }
//        decouplePool.execute(r);
        //findAndProcessPendingMessages(name);
        return ret;
    }

    public void findAndProcessPendingMessages(String name) {
        Runnable r = () -> {
            while (true) {
                MorphiumIterator<Msg> messages = lockAndGetMessages(name, processMultiple);
                if (!messages.hasNext()) break;
                processMessages(messages);
                try {
                    Thread.sleep(pause);
                } catch (InterruptedException e) {
                    //Swallow
                }
            }
        };
        queueOrRun(r);

    }

    private MorphiumIterator<Msg> lockAndGetMessages(String name, boolean multiple) {

        Map<String, Object> values = new HashMap<>();
        Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName());
        if (listenerByName.isEmpty() && listeners.isEmpty()) {
            //No listeners - only answers will be processed
            return q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.inAnswerTo).in(waitingForMessages.keySet()).asIterable();
        }
        //locking messages..
        if (!useChangeStream) {
            q.f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).in(Arrays.asList(id, null)).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipients).in(Arrays.asList(null, id));

        } else {
            q.f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).in(Arrays.asList(id, null, "ALL")).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipients).in(Arrays.asList(null, id));
        }

        Set<String> pausedMessagesKeys = pauseMessages.keySet();
        if (name != null) {
            q.f(Msg.Fields.name).eq(name);

        } else {
            //not searching for paused messages
            if (!pauseMessages.isEmpty()) {
                q.f(Msg.Fields.name).nin(pausedMessagesKeys);

            }
            if (listeners.isEmpty() && !listenerByName.isEmpty()) {
                q.f(Msg.Fields.name).in(listenerByName.keySet());

            }
        }
        ArrayList<MorphiumId> processingIds = new ArrayList<>(processing);
        if (!processing.isEmpty()) {
            q.f("_id").nin(processingIds);
        }

        q.sort(Msg.Fields.priority, Msg.Fields.timestamp);
        if (!multiple) {
            q.limit(1);
        } else {
            int locked = (int) morphium.createQueryFor(Msg.class, getCollectionName())
                    .f(Msg.Fields.sender).ne(id)
                    .f(Msg.Fields.lockedBy).eq(id)
                    .f(Msg.Fields.processedBy).ne(id).countAll();
            if (locked >= windowSize) {
                //q.limit(0);
                return new EmptyIterator<>();
            } else {
                q.limit(windowSize - locked);
            }
        }
        if (!useChangeStream) {
            values.put("locked_by", id);
        }
        values.put("locked", System.currentTimeMillis());
        //just trigger unprocessed messages for Changestream...
        long cnt = q.countAll();
        List<Object> lst = q.idList();
        if (cnt > q.idList().size()) {
            skipped.incrementAndGet();
        }
        morphium.set(q.q().f("_id").in(lst), values, false, multiple);

        if (!useChangeStream) {
            try {
                //waiting a bit for data to be stored
                Thread.sleep(10);
            } catch (InterruptedException e) {
                //swallow
            }

            q = q.q();
            q.f(Msg.Fields.sender).ne(id);
            q.f("_id").nin(processingIds);
            q.f(Msg.Fields.lockedBy).in(Arrays.asList("ALL", id)).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipients).in(Arrays.asList(null, id));
            if (name != null) {
                q.f(Msg.Fields.name).eq(name);
            } else {
                //not searching for paused messages
                if (!pauseMessages.isEmpty()) {
                    q.f(Msg.Fields.name).nin(pausedMessagesKeys);
                }
            }
            q.sort(Msg.Fields.priority, Msg.Fields.timestamp);
            if (!multiple) {
                q.limit(1);
            } else {
                q.limit(windowSize);
            }
            //                List<Msg> messages = q.asList();
            MorphiumIterator<Msg> it = q.asIterable(windowSize);
            it.setMultithreaddedAccess(multithreadded);
            return it;
        } else {
            return new EmptyIterator<>();
        }
    }

    private void findAndProcessMessages(boolean multiple) {
        MorphiumIterator<Msg> messages = lockAndGetMessages(null, multiple);
        processMessages(messages);
    }

    private void lockAndProcess(Msg obj) {

        Query<Msg> q = morphium.createQueryFor(Msg.class, getCollectionName());
        q.f(Msg.Fields.sender).ne(id);
        q.f(Msg.Fields.lockedBy).eq(id).f(Msg.Fields.processedBy).ne(id);
        if (processMultiple && q.countAll() >= windowSize) {
            skipped.incrementAndGet();
            return; //not locking - windowsize reached!
        }
        q.f(Msg.Fields.msgId).eq(obj.getMsgId());
        if (!processMultiple && q.countAll() > 1) {
            skipped.incrementAndGet();
            return; //already processing one
        }

        q = q.q();
        q.f("_id").eq(obj.getMsgId());
        q.f(Msg.Fields.processedBy).ne(id);
        q.f(Msg.Fields.lockedBy).in(Arrays.asList(null, id));
        Map<String, Object> values = new HashMap<>();
        values.put("locked_by", id);
        values.put("locked", System.currentTimeMillis());
        morphium.set(q, values, false, false); //always locking single message
        try {
            //wait for the locking to be saved
            Thread.sleep(10);
        } catch (InterruptedException e) {
            //swallow
        }
        obj = morphium.reread(obj, getCollectionName());
        if (obj != null && obj.getLockedBy() != null && obj.getLockedBy().equals(id)) {
//            if (log.isDebugEnabled())
//                log.debug("locked messages: " + lst.size());
            try {
                processMessages(Arrays.asList(obj));
            } catch (Exception e) {
                log.error("Error during message processing ", e);
            }
        }
    }

    private synchronized void processMessages(Iterable<Msg> messages) {
//        final List<Msg> toStore = new ArrayList<>();
//        final List<Runnable> toExec = new ArrayList<>();
        for (final Msg me : messages) {
            if (me == null) {
                continue; //message was erased
            }
            final Msg msg = morphium.reread(me, getCollectionName()); //make sure it's current version in DB
            if (msg == null) {
                processing.remove(me.getMsgId());
                continue;
            }
            if (!msg.getLockedBy().equals("ALL") && !msg.getLockedBy().equals(id)) {
                //overlocked
                processing.remove(me.getMsgId());
                continue;
            }
            if (msg.isExclusive() && msg.getProcessedBy().contains(id)) {
                morphium.unset(msg, Msg.Fields.lockedBy);
                processing.remove(msg.getMsgId());
                continue;
            }


            if (msg.getProcessedBy().contains(getSenderId())) {
                processing.remove(msg.getMsgId());
                continue;
            }
            if (msg.isExclusive() && !getSenderId().equals(msg.getLockedBy())) {
                processing.remove(msg.getMsgId());
                continue;
            }
            if (msg.getSender().equals(getSenderId())) {
                processing.remove(msg.getMsgId());
                continue;
            }

            //noinspection SuspiciousMethodCalls
            if (msg.getInAnswerTo() != null && waitingForMessages.containsKey(msg.getInAnswerTo())) {
                if (log.isDebugEnabled())
                    log.debug(getSenderId() + ": Got a message, we are waiting for...");
                //this message we were waiting for
                updateProcessedBy(msg);
                waitingForAnswers.put((MorphiumId) msg.getInAnswerTo(), msg);
                if (receiveAnswers.equals(ReceiveAnswers.NONE)) {
                    processing.remove(msg.getMsgId());
                    //morphium.delete(msg, getCollectionName());
                    msg.addProcessedId(id);
                    morphium.store(msg, getCollectionName(), null);
                    return;
                }
            }

            if (msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
                //Delete outdated msg!
                if (log.isDebugEnabled())
                    log.debug(getSenderId() + ": Found outdated message - deleting it!");
                morphium.delete(msg, getCollectionName());
                processing.remove(msg.getMsgId());
                return;
            }

            if (listeners.isEmpty() && !listenerByName.containsKey(msg.getName()) && msg.getInAnswerTo() == null) {
                updateProcessedBy(msg);
                if (msg.isExclusive()) {
                    morphium.unset(msg, getCollectionName(), Msg.Fields.lockedBy);
                }
                log.error(getSenderId() + ": should not be here. not processing message, as no listeners are defined " + msg.getMsgId());
                return;
            }

            if (processing.contains(msg.getMsgId())) continue;
            processing.add(msg.getMsgId());

            Runnable r = () -> {
                boolean wasProcessed = false;
                boolean wasRejected = false;
                List<MessageRejectedException> rejections = new ArrayList<>();
                List<MessageListener> lst = new ArrayList<>(listeners);
                if (listenerByName.get(msg.getName()) != null) {
                    lst.addAll(listenerByName.get(msg.getName()));
                }
                if (lst.isEmpty()) {
                    if (log.isDebugEnabled())
                        log.debug(getSenderId() + ": Message did not have a listener registered for name " + msg.getName());
                    wasProcessed = true;
                }
                for (MessageListener l : lst) {
                    Msg msg1 = msg;
                    if (msg1 == null || msg1.isExclusive() && !msg1.getLockedBy().equals(id)) {
                        log.error("msg was overlocked or deleted?!?!?");
                        wasProcessed = true;
                        break;
                    } else {
                        try {
                            if (pauseMessages.containsKey(msg1.getName())) {
                                //paused - do not process
                                processing.remove(msg1.getMsgId());
                                skipped.incrementAndGet();
                                return;
                            }
                            Msg answer = l.onMessage(Messaging.this, msg1);
                            wasProcessed = true;
                            if (autoAnswer && answer == null) {
                                answer = new Msg(msg1.getName(), "received", "");
                            }
                            if (answer != null) {
                                msg1.sendAnswer(Messaging.this, answer);
//                            if (log.isDebugEnabled())
//                                log.debug("sent answer to " + answer.getInAnswerTo() + " recipient: " + answer.getRecipient() + " id: " + answer.getMsgId());
                                if (answer.getRecipients() == null) {
                                    log.warn("Recipient of answer is null?!?!");

                                }
                            }
                        } catch (MessageRejectedException mre) {
                            log.warn("Message was rejected by listener", mre);
                            wasRejected = true;
                            rejections.add(mre);
                        } catch (Exception e) {
                            log.error("listener Processing failed", e);
                        }
                    }

                }
                if (wasRejected) {
                    for (MessageRejectedException mre : rejections) {
                        if (mre.isSendAnswer()) {
                            Msg answer = new Msg(msg.getName(), "message rejected by listener", mre.getMessage());
                            msg.sendAnswer(Messaging.this, answer);

                        }
                        if (mre.isContinueProcessing()) {
                            updateProcessedBy(msg);
                            morphium.unset(msg, getCollectionName(), Msg.Fields.lockedBy);
                            processing.remove(msg.getMsgId());

                            log.debug("Message will be re-processed by others");
                        }
                    }
                }
                if (!wasProcessed && !wasRejected) {
                    //                        msg.addAdditional("Processing of message failed by "+getSenderId()+": "+t.getMessage());

                    log.error("message was not processed");
                    if (msg.isExclusive()) {
                        msg.setLocked(0);
                        msg.setLockedBy(null);
                        morphium.store(msg, getCollectionName(), null);
                    }
                } else if (wasRejected) {
                    log.debug("Message rejected");
                }

                //                            if (msg.getType().equals(MsgType.SINGLE)) {
                //                                //removing it
                //                                morphium.delete(msg, getCollectionName());
                //                            }
                //updating it to be processed by others...
                if (wasProcessed) {

                    updateProcessedBy(msg);

                }
                Runnable rb = new RemoveProcessTask(processing, msg.getMsgId());
                while (true) {
                    try {
                        if (!decouplePool.isTerminated() && !decouplePool.isTerminating() && !decouplePool.isShutdown()) {
                            decouplePool.schedule(rb, msg.getTtl(), TimeUnit.MILLISECONDS); //avoid re-executing message
                        }
                        break;
                    } catch (RejectedExecutionException ex) {
                        try {
                            Thread.sleep(pause);
                        } catch (InterruptedException e) {
                            //Swallow
                        }
                    }
                }
            };


            queueOrRun(r);

        }

        //wait for all threads to finish
//        if (multithreadded) {
//            while (threadPool != null && threadPool.getActiveCount() > 0) {
//                Thread.yield();
//            }
//        }
//        morphium.storeList(toStore, getCollectionName());
//        morphium.delete(toRemove, getCollectionName());
//        toExec.forEach(this::queueOrRun);
        while (morphium.getWriteBufferCount() > 0) {
            Thread.yield();
        }
    }


    private void updateProcessedBy(Msg msg) {
//        Query<Msg> idq = morphium.createQueryFor(Msg.class);
//        idq.setCollectionName(getCollectionName());
//        idq.f(Msg.Fields.msgId).eq(msg.getMsgId());
//        idq.f(Msg.Fields.processedBy).ne(id);
//        morphium.push(idq, Msg.Fields.processedBy, id);
//        if (msg.getLockedBy() != null && msg.getLockedBy().equals(id)) {
//            //releasing lock
//            morphium.unsetQ(idq.q().f(Msg.Fields.msgId).eq(msg.getMsgId()), Msg.Fields.lockedBy);
//        }
        //msg.setLockedBy(null);
        Query<Msg> idq = morphium.createQueryFor(Msg.class, getCollectionName());
        idq.f(Msg.Fields.msgId).eq(msg.getMsgId())
                .f(Msg.Fields.processedBy).ne(id); //avoid duplication
        if (msg.getProcessedBy() == null) msg.setProcessedBy(new ArrayList<>());
        if (!msg.getProcessedBy().contains(id)) msg.getProcessedBy().add(id);
        morphium.push(idq, Msg.Fields.processedBy, id);
    }

    private void queueOrRun(Runnable r) {
        if (multithreadded) {
            boolean queued = false;
            while (!queued) {
                try {
                    //throtteling to windowSize - do not create more threads than windowSize
                    while (threadPool.getActiveCount() > windowSize) {
                        Thread.yield();
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
        if (queueName == null || queueName.isEmpty()) {
            return "msg";
        }
        return "mmsg_" + queueName;

    }

    public void addListenerForMessageNamed(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            HashMap<String, List<MessageListener>> c = (HashMap) ((HashMap) listenerByName).clone();
            c.put(n, new ArrayList<>());
            listenerByName = c;
        }
        listenerByName.get(n).add(l);
    }

    public void removeListenerForMessageNamed(String n, MessageListener l) {
        //        l.setMessaging(null);
        if (listenerByName.get(n) == null) {
            return;
        }
        HashMap<String, List<MessageListener>> c = (HashMap) ((HashMap) listenerByName).clone();
        c.get(n).remove(l);
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
        if (decouplePool != null) {
            int sz = decouplePool.shutdownNow().size();
//            if (log.isDebugEnabled())
//                log.debug("Shutting down with " + sz + " runnables still scheduled");
        }
        if (threadPool != null) {
            int sz = threadPool.shutdownNow().size();
//            if (log.isDebugEnabled())
//                log.debug("Shutting down with " + sz + " runnables still pending in pool");
        }
        if (changeStreamMonitor != null) changeStreamMonitor.terminate();
        if (isAlive()) {
            interrupt();
        }
        int retry = 0;
        while (isAlive()) {
            try {
                sleep(150);
            } catch (InterruptedException e) {
                //swallow
            }
            retry++;
            if (retry > morphium.getConfig().getMaxWaitTime() / 150) {
                log.warn("Force stopping messaging!");
                stop();
            }
            if (retry > 2 * morphium.getConfig().getMaxWaitTime() / 150)
                throw new RuntimeException("Could not terminate Messaging!");
        }
    }

    public void addMessageListener(MessageListener l) {
        listeners.add(l);
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
        if (useChangeStream) {
            try {
                Thread.sleep(250);
                //wait for changestream to kick in ;-)
            } catch (Exception e) {
                log.error("error:" + e.getMessage());
            }
        }
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
            //noinspection unused,unused
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
        //m.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl()));
        m.setSender(id);
        //m.addProcessedId(id);

        m.setSenderHost(hostname);
//        if (m.getTo() != null && !m.getTo().isEmpty()) {
//            for (String recipient : m.getTo()) {
//                try {
//                    Msg msg = m.getClass().getDeclaredConstructor().newInstance();
//                    List<Field> flds = morphium.getARHelper().getAllFields(m.getClass());
//                    for (Field f : flds) {
//                        f.setAccessible(true);
//                        f.set(msg, f.get(m));
//                    }
//                    msg.setMsgId(null);
//                    msg.setRecipient(recipient);
//                    morphium.storeNoCache(msg, getCollectionName(), cb);
//                } catch (Exception e) {
//                    throw new RuntimeException("Sending of answer failed", e);
//                }
//            }
//        } else {
        morphium.storeNoCache(m, getCollectionName(), cb);
//        }
    }

    public void sendMessageToSelf(Msg m) {
        sendMessageToSelf(m, false);
    }

    public void queueMessagetoSelf(Msg m) {
        sendMessageToSelf(m, true);
    }

    private void sendMessageToSelf(Msg m, boolean async) {
        AsyncOperationCallback cb = null;
        //noinspection StatementWithEmptyBody
        if (async) {
            //noinspection unused,unused
        }
        m.setSender("self");
        m.addRecipient(id);
        m.setSenderHost(hostname);
        morphium.storeNoCache(m, getCollectionName());
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
            if (changeStreamMonitor != null) changeStreamMonitor.terminate();
        } catch (Exception e) {
            e.printStackTrace();
            //swallow
        }
    }

    public <T extends Msg> T sendAndAwaitFirstAnswer(T theMessage, long timeoutInMs) {
        theMessage.setMsgId(new MorphiumId());
        waitingForMessages.put(theMessage.getMsgId(), theMessage);
        sendMessage(theMessage);
        long start = System.currentTimeMillis();
        while (!waitingForAnswers.containsKey(theMessage.getMsgId())) {
            if (System.currentTimeMillis() - start > timeoutInMs) {
                log.error("Did not receive answer " + theMessage.getName() + "/" + theMessage.getMsgId() + " in time (" + timeoutInMs + "ms)");
                waitingForMessages.remove(theMessage.getMsgId());
                throw new RuntimeException("Did not receive answer for message " + theMessage.getName() + "/" + theMessage.getMsgId() + " in time (" + timeoutInMs + "ms)");
            }
            Thread.yield();
        }
        if (log.isDebugEnabled()) {
            log.debug("got message after: " + (System.currentTimeMillis() - start) + "ms");
        }
        waitingForMessages.remove(theMessage.getMsgId());
        return (T) waitingForAnswers.remove(theMessage.getMsgId());
    }

    public <T extends Msg> List<T> sendAndAwaitAnswers(T theMessage, int numberOfAnswers, long timeout) {
        if (theMessage.getMsgId() == null)
            theMessage.setMsgId(new MorphiumId());
        waitingForMessages.put(theMessage.getMsgId(), theMessage);
        List<Msg> answers = new ArrayList<>();
        sendMessage(theMessage);
        long start = System.currentTimeMillis();
        while (true) {
            if (waitingForAnswers.get(theMessage.getMsgId()) != null) {
                answers.add(waitingForAnswers.remove(theMessage.getMsgId()));
            }
            if (numberOfAnswers >= 0 && answers.size() >= numberOfAnswers) {
                break;
            }
            if (System.currentTimeMillis() - start > timeout) {
                break;
            }
            Thread.yield();
        }
        waitingForMessages.remove(theMessage.getMsgId());
        return (List<T>) answers;
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
        return this;
    }

    public boolean isMultithreadded() {
        return multithreadded;
    }

    public Messaging setMultithreadded(boolean multithreadded) {
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

    public Messaging setUseChangeStream(boolean useChangeStream) {
        this.useChangeStream = useChangeStream;
        return this;
    }


    public enum ReceiveAnswers {
        NONE, ONLY_MINE, ALL,
    }
}
