package de.caluga.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:48
 * <p/>
 * Messaging implements a simple, threadsafe and messaging api. Used for cache synchronization.
 */
@SuppressWarnings({"ConstantConditions", "unchecked", "UnusedDeclaration"})
public class Messaging extends Thread {
    private static Logger log = Logger.getLogger(Messaging.class);

    private Morphium morphium;
    private boolean running;
    private int pause = 5000;
    private String id;
    private boolean autoAnswer = false;
    private boolean burstMode = false;
    private boolean processMultiple = false;

    private List<MessageListener> listeners;
    private Map<String, List<MessageListener>> listenerByName;

    private String queueName;

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


    public Messaging(Morphium m, String queueName, int pause, boolean processMultiple) {
        this.queueName = queueName;
        morphium = m;
        running = true;
        this.pause = pause;
        this.processMultiple = processMultiple;
        id = UUID.randomUUID().toString();
        m.ensureIndicesFor(Msg.class, queueName);
//        try {
//            m.ensureIndex(Msg.class, Msg.Fields.lockedBy, Msg.Fields.timestamp);
//            m.ensureIndex(Msg.class, Msg.Fields.lockedBy, Msg.Fields.processedBy);
//            m.ensureIndex(Msg.class, Msg.Fields.timestamp);
//        } catch (Exception e) {
//            log.error("Could not ensure indices", e);
//        }

        listeners = new Vector<MessageListener>();
        listenerByName = new Hashtable<String, List<MessageListener>>();
    }

    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Messaging " + id + " started");
        }
        Map<String, Object> values = new HashMap<String, Object>();
        boolean processed = false;
        while (running) {

            try {
                Query<Msg> q = morphium.createQueryFor(Msg.class);
                q.setCollectionName(getCollectionName());
                if (q.countAll() == 0) {
                    sleep(pause);
                    continue;
                }

                //removing all outdated stuff
                q = q.where("this.ttl<" + System.currentTimeMillis() + "-this.timestamp");
                if (log.isDebugEnabled() && q.countAll() > 0) {
                    log.debug("Deleting outdate messages: " + q.countAll());
                }
                morphium.delete(q);
                q = q.q();
                if (q.countAll() == 0) {
                    sleep(pause);
                    continue;
                }

                //locking messages...
                long start = System.currentTimeMillis();
                q.or(q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq("").f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(""),
                        q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq("").f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(id));
                values.put("locked_by", id);
                values.put("locked", System.currentTimeMillis());
                morphium.set(q, values, false, processMultiple);
                long dur = System.currentTimeMillis() - start;
//                log.warn("locking took " + dur + " ms");
                q = q.q();
                q.or(q.q().f(Msg.Fields.lockedBy).eq(id),
                        q.q().f(Msg.Fields.lockedBy).eq("ALL").f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(id),
                        q.q().f(Msg.Fields.lockedBy).eq("ALL").f(Msg.Fields.processedBy).ne(id).f(Msg.Fields.recipient).eq(""));
                q.sort(Msg.Fields.timestamp);

                List<Msg> messagesList = q.asList();

                for (Msg msg : messagesList) {
                    processed = true;
                    msg = morphium.reread(msg, getCollectionName()); //make sure it's current version in DB
                    if (msg == null) continue; //was deleted
                    if (!msg.getLockedBy().equals(id) && !msg.getLockedBy().equals("ALL")) {
                        //over-locked by someone else
                        continue;
                    }
                    if (msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
                        //Delete outdated msg!
                        log.info("Found outdated message - deleting it!");
                        morphium.delete(msg, getCollectionName());
                        continue;
                    }
                    try {
                        for (MessageListener l : listeners) {
                            Msg answer = l.onMessage(this, msg);
                            if (autoAnswer && answer == null) {
                                answer = new Msg(msg.getName(), "received", "");
                            }
                            if (answer != null) {
                                msg.sendAnswer(this, answer);
                            }
                        }

                        if (listenerByName.get(msg.getName()) != null) {
                            for (MessageListener l : listenerByName.get(msg.getName())) {
                                Msg answer = l.onMessage(this, msg);
                                if (autoAnswer && answer == null) {
                                    answer = new Msg(msg.getName(), "received", "");
                                }
                                if (answer != null) {
                                    msg.sendAnswer(this, answer);
                                }
                            }
                        }
                    } catch (Throwable t) {
//                        msg.addAdditional("Processing of message failed by "+getSenderId()+": "+t.getMessage());
                        log.error("Processing failed", t);
                    }

                    if (msg.getType().equals(MsgType.SINGLE)) {
                        //removing it
                        morphium.delete(msg, getCollectionName());
                    } else {
                        //updating it to be processed by others...
                        Query<Msg> idq = MorphiumSingleton.get().createQueryFor(Msg.class);
                        idq.setCollectionName(getCollectionName());
                        idq.f(Msg.Fields.msgId).eq(msg.getMsgId());

                        MorphiumSingleton.get().push(idq, Msg.Fields.processedBy, id);
                        if (msg.getLockedBy().equals("ALL")) {
                            //nothing
                        } else {
                            //Exclusive message
                            Map<String, Object> update = new HashMap<String, Object>();
                            update.put(Msg.Fields.locked.name(), 0);
                            update.put(Msg.Fields.lockedBy.name(), "");

                            MorphiumSingleton.get().set(idq, update, false, false);
                        }
                    }

                }
            } catch (Throwable e) {
                log.error("Unhandled exception " + e.getMessage(), e);
            } finally {
                try {
                    if ((!processed && burstMode) || !burstMode) {
//                        log.info("sleeping");
                        sleep(pause);
                    }
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

    public String getCollectionName() {
        if (queueName == null || queueName.isEmpty()) {
            return "msg";
        }
        return "mmsg_" + queueName;

    }

    public void addListenerForMessageNamed(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            listenerByName.put(n, new ArrayList<MessageListener>());
        }
        listenerByName.get(n).add(l);
    }

    public void removeListenerForMessageNamed(String n, MessageListener l) {
//        l.setMessaging(null);
        if (listenerByName.get(n) == null) {
            return;
        }
        listenerByName.get(n).remove(l);

    }

    public boolean isBurstMode() {
        return burstMode;
    }

    /**
     * when one message is processed, start immediately with the next one
     *
     * @param burstMode
     */
    public void setBurstMode(boolean burstMode) {
        this.burstMode = burstMode;
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

    private void storeMsg(Msg m, boolean async) {
        AsyncOperationCallback cb = null;
        if (async) {
            cb = new AsyncOperationCallback() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query q, long duration, List result, Object entity, Object... param) {
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query q, long duration, String error, Throwable t, Object entity, Object... param) {
                }
            };
        }
        m.setSender(id);
        m.addProcessedId(id);
        m.setLockedBy("");
        m.setLocked(0);
        if (m.getTo() != null && m.getTo().size() > 0) {
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
}
