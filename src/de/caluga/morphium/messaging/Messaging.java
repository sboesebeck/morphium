package de.caluga.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Query;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:48
 * <p/>
 * TODO: Add documentation here
 */
public class Messaging extends Thread {
    private static Logger log = Logger.getLogger(Messaging.class);

    private Morphium morphium;
    private boolean running;
    private int pause = 5000;
    private String id;

    private boolean processMultiple = false;

    private List<MessageListener> listeners;
    private Map<String, List<MessageListener>> listenerByName;


    public Messaging(Morphium m, int pause, boolean processMultiple) {
        morphium = m;
        running = true;
        this.pause = pause;
        this.processMultiple = processMultiple;
        id = UUID.randomUUID().toString();
        m.ensureIndex(Msg.class, Msg.Fields.lockedBy, Msg.Fields.timestamp);
        m.ensureIndex(Msg.class, Msg.Fields.lockedBy, Msg.Fields.lstOfIdsAlreadyProcessed);
        m.ensureIndex(Msg.class, Msg.Fields.timestamp);

        listeners = new Vector<MessageListener>();
        listenerByName = new Hashtable<String, List<MessageListener>>();
//        start();
    }

    public void run() {
        Map<String, Object> values = new HashMap<String, Object>();
        while (running) {
            Query<Msg> q = morphium.createQueryFor(Msg.class);
            //removing all outdated stuff
            q = q.where("this.ttl<" + System.currentTimeMillis() + "-this.timestamp");
            if (log.isDebugEnabled()) {
                log.info("Deleting outdate messages: " + q.countAll());
            }
            morphium.delete(q);
            q=q.q();
            //locking messages...
            q.or(q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq(null).f(Msg.Fields.lstOfIdsAlreadyProcessed).ne(id).f(Msg.Fields.to).eq(null),
                    q.q().f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq(null).f(Msg.Fields.lstOfIdsAlreadyProcessed).ne(id).f(Msg.Fields.to).eq(id));
            values.put("locked_by", id);
            values.put("locked", System.currentTimeMillis());
//            morphium.set(Msg.class,q,Msg.Fields.lockedBy,id);
            morphium.set(Msg.class, q, values, false, processMultiple);
            //give mongo time to really store

            try {
                sleep(500);
            } catch (InterruptedException e) {
            }
            //maybe others "overlocked" our message, but that's ok - we re read all messages...
            q = q.q();
            q = q.f(Msg.Fields.lockedBy).eq(id);
            q.sort(Msg.Fields.timestamp);

            List<Msg> messagesList = q.asList();
            List<Msg> toStore=new ArrayList<Msg>();

            for (Msg msg : messagesList) {
                msg = morphium.reread(msg); //make sure it's current version in DB
                if (msg==null) continue; //was deleted
                if (!msg.getLockedBy().equals(id)) {
                    //over-locked by someone else
                    continue;
                }
                if (msg.getTtl() < System.currentTimeMillis() - msg.getTimestamp()) {
                    //Delete outdated msg!
                    log.warn("Found outdated message - deleting it!");
                    morphium.deleteObject(msg);
                    continue;
                }
                for (MessageListener l : listeners) {
                    l.onMessage(msg);
                }

                if (listenerByName.get(msg.getName()) != null) {
                    for (MessageListener l : listenerByName.get(msg.getName())) {
                        l.onMessage(msg);
                    }
                }

                if (msg.getType().equals(MsgType.SINGLE)) {
                    //removing it
                    morphium.deleteObject(msg);
                }
                //updating it to be processed by others...
                msg.addProcessedId(id);
                msg.setLockedBy(null);
                msg.setLocked(0);
                toStore.add(msg);

            }
            morphium.storeList(messagesList);
            try {
                sleep(pause);
            } catch (InterruptedException e) {
            }
        }
    }

    public void addListenerForMessageNamed(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            listenerByName.put(n, new ArrayList<MessageListener>());
        }
        listenerByName.get(n).add(l);
    }

    public void removeListenerForMessageNamed(String n, MessageListener l) {
        if (listenerByName.get(n) == null) {
            return;
        }
        listenerByName.get(n).remove(l);
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

    public void queueMessage(Msg m) {
        m.setSender(id);
        m.addProcessedId(id);
        morphium.store(m);
    }

}
