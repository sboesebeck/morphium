package de.caluga.morphium.messaging;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;

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
 */
@SuppressWarnings({"ConstantConditions", "unchecked", "UnusedDeclaration"})
public class Messaging extends Thread implements ShutdownListener {
    private static Logger log = new Logger(Messaging.class);

    private Morphium morphium;
    private boolean running;
    private int pause = 5000;
    private String id;
    private boolean autoAnswer = false;
    private String hostname;
    private boolean processMultiple = false;

    private List<MessageListener> listeners;

    private Map<String, List<MessageListener>> listenerByName;
    private String queueName;

    private ThreadPoolExecutor threadPool;

    private boolean multithreadded = false;
    private int windowSize = 1000;


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

    public Messaging(Morphium m, int pause, boolean processMultiple, boolean multithreadded, int windowSize) {
        this(m, null, pause, processMultiple, multithreadded, windowSize);
    }

    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple) {
        this(m, queueName, pause, processMultiple, false, 1000);
    }

    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple, boolean multithreadded, int windowSize) {
        this.multithreadded = multithreadded;
        this.windowSize = windowSize;
        morphium = m;


        if (multithreadded) {
            threadPool = new ThreadPoolExecutor(morphium.getConfig().getThreadPoolMessagingCoreSize(), morphium.getConfig().getThreadPoolMessagingMaxSize(),
                    morphium.getConfig().getThreadPoolMessagingKeepAliveTime(), TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
            //noinspection unused,unused
            threadPool.setThreadFactory(new ThreadFactory() {
                private AtomicInteger num = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread ret = new Thread(r, "messaging " + num);
                    num.set(num.get() + 1);
                    ret.setDaemon(true);
                    return ret;
                }
            });
        }
        morphium.addShutdownListener(this);

        this.queueName = queueName;
        running = true;
        this.pause = pause;
        this.processMultiple = processMultiple;
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

        m.ensureIndicesFor(Msg.class, queueName);
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

    public long getMessageCount() {
        return morphium.createQueryFor(Msg.class).setCollectionName(getCollectionName()).countAll();
    }

    public void removeMessage(Msg m) {
        morphium.delete(m, getCollectionName());
    }

    public List<Msg> findMessages(Query<Msg> q) {
        try {
            q = q.clone();
        } catch (CloneNotSupportedException e) {
            //cannot happen
        }
        q.setCollectionName(getCollectionName());
        return q.asList();
    }

    @Override
    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Messaging " + id + " started");
        }
        Map<String, Object> values = new HashMap<>();
        while (running) {

            try {
                Query<Msg> q = morphium.createQueryFor(Msg.class);
                q.setCollectionName(getCollectionName());
                //                //removing all outdated stuff
                //                q = q.where("this.ttl<" + System.currentTimeMillis() + "-this.timestamp");
                //                if (log.isDebugEnabled() && q.countAll() > 0) {
                //                    log.debug("Deleting outdate messages: " + q.countAll());
                //                }
                //                morphium.remove(q);
                //                q = q.q();
                //locking messages...
                q.or(q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq(null).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(null),
                        q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq(null).f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(id));
                values.put("locked_by", id);
                values.put("locked", System.currentTimeMillis());
                morphium.set(q, values, false, processMultiple);
                q = q.q();
                q.or(q.q().f(Msg.Fields.lockedBy).eq(id),
                        q.q().f(Msg.Fields.lockedBy).eq("ALL").f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(id),
                        q.q().f(Msg.Fields.lockedBy).eq("ALL").f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(null));
                q.sort(Msg.Fields.timestamp);

                //                List<Msg> messages = q.asList();
                MorphiumIterator<Msg> messages = q.asIterable(windowSize);
                messages.setMultithreaddedAccess(multithreadded);

                final List<Msg> toStore = new ArrayList<>();
                final List<Runnable> toExec = new ArrayList<>();
                final List<Msg> toRemove = new ArrayList<>();
                //                int count=0;
                for (final Msg m : messages) {
                    //                    count++;
                    //                    System.out.println("Processing message " + count);
                    if (m == null) {
                        continue; //message was erased
                    }
                    Runnable r = () -> {
                        if (m.getProcessedBy() != null && m.getProcessedBy().contains(id)) {
                            log.fatal("Was already processed - ERROR?");
                            throw new RuntimeException("was already processed - error on mongo query result!");
                        }
                        final Msg msg = morphium.reread(m, getCollectionName()); //make sure it's current version in DB
                        //                            System.out.println("Processing message "+msg.getMsgId()+ " / "+m.getMsgId());
                        if (msg == null) {
                            return; //was deleted
                        }
                        if (msg.getProcessedBy() != null && msg.getProcessedBy().contains(id)) {
                            log.info("Was already processed - multithreadding?");
                            return;
                        }
                        if (!msg.getLockedBy().equals(id) && !msg.getLockedBy().equals("ALL")) {
                            //over-locked by someone else
                            return;
                        }
                        if (msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
                            //Delete outdated msg!
                            log.info("Found outdated message - deleting it!");
                            morphium.delete(msg, getCollectionName());
                            return;
                        }
                        try {
                            for (MessageListener l : listeners) {
                                Msg answer = l.onMessage(Messaging.this, msg);
                                if (autoAnswer && answer == null) {
                                    answer = new Msg(msg.getName(), "received", "");
                                }
                                if (answer != null) {
                                    msg.sendAnswer(Messaging.this, answer);
                                }
                            }

                            if (listenerByName.get(msg.getName()) != null) {
                                for (MessageListener l : listenerByName.get(msg.getName())) {
                                    Msg answer = l.onMessage(Messaging.this, msg);
                                    if (autoAnswer && answer == null) {
                                        answer = new Msg(msg.getName(), "received", "");
                                    }
                                    if (answer != null) {
                                        msg.setDeleteAt(new Date(System.currentTimeMillis() + msg.getTtl()));
                                        msg.sendAnswer(Messaging.this, answer);
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            //                        msg.addAdditional("Processing of message failed by "+getSenderId()+": "+t.getMessage());
                            log.error("Processing failed", t);
                        }

                        //                            if (msg.getType().equals(MsgType.SINGLE)) {
                        //                                //removing it
                        //                                morphium.delete(msg, getCollectionName());
                        //                            }
                        //updating it to be processed by others...
                        if (msg.getLockedBy().equals("ALL")) {
                            toExec.add(() -> {
                                Query<Msg> idq = morphium.createQueryFor(Msg.class);
                                idq.setCollectionName(getCollectionName());
                                idq.f(Msg.Fields.msgId).eq(msg.getMsgId());

                                morphium.push(idq, Msg.Fields.processedBy, id);
                            });

                        } else {
                            //Exclusive message

                            toRemove.add(msg);
                            //                                msg.addProcessedId(id);
                            //                                msg.setLockedBy(null);
                            //                                msg.setLocked(0);
                            //                                toStore.add(msg);
                        }
                        if (msg.getType().equals(MsgType.SINGLE) && !toRemove.contains(msg)) {
                            toRemove.add(msg);
                        }
                    };


                    queueOrRun(r);
                }

                //wait for all threads to finish
                if (multithreadded) {
                    while (threadPool != null && threadPool.getActiveCount() > 0) {
                        Thread.sleep(100);
                    }
                }
                morphium.storeList(toStore, getCollectionName());
                morphium.delete(toRemove, getCollectionName());
                toExec.forEach(this::queueOrRun);
                while (morphium.getWriteBufferCount() > 0) {
                    Thread.sleep(100);
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

    private void queueOrRun(Runnable r) {
        if (multithreadded) {
            boolean queued = false;
            while (!queued) {
                try {
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

    public void setSenderId(String id) {
        this.id = id;
    }

    public int getPause() {
        return pause;
    }

    public void setPause(int pause) {
        this.pause = pause;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;

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

    public void storeMessage(Msg m) {
        storeMsg(m, false);
    }

    public long getNumberOfMessages() {
        Query<Msg> q = morphium.createQueryFor(Msg.class);
        q.setCollectionName(getCollectionName());
        return q.countAll();
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
                }
            };
        }
        m.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl()));
        m.setSender(id);
        m.addProcessedId(id);
        m.setLockedBy(null);
        m.setLocked(0);
        m.setSenderHost(hostname);
        if (m.getTo() != null && !m.getTo().isEmpty()) {
            for (String recipient : m.getTo()) {
                Msg msg = m.getCopy();
                msg.setRecipient(recipient);
                morphium.storeNoCache(msg, getCollectionName(), cb);
            }
        } else {
            morphium.storeNoCache(m, getCollectionName(), cb);
        }
    }

    public boolean isAutoAnswer() {
        return autoAnswer;
    }

    public void setAutoAnswer(boolean autoAnswer) {
        this.autoAnswer = autoAnswer;
    }

    @Override
    public void onShutdown(Morphium m) {
        try {
            if (threadPool != null) {
                threadPool.shutdownNow();
                threadPool = null;
            }
        } catch (Exception e) {
            //swallow
        }
    }
}
