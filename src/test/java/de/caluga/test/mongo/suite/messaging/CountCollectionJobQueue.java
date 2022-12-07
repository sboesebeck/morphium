
package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.annotations.DefaultReadPreference;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.locking.Lockable;
import de.caluga.morphium.annotations.locking.LockedAt;
import de.caluga.morphium.annotations.locking.LockedBy;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class CountCollectionJobQueue extends MorphiumTestBase {

    @Test
    public void rejectPauseReplayTest() throws Exception {
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Vector<Messaging> clients = new Vector<>();
        Map<String, Integer> maxParallel = Map.of("msg1", 1, "msg3", 15, "msg4", 12, "msg5", 3);
        AtomicInteger counts = new AtomicInteger(0);
        AtomicInteger locks = new AtomicInteger(0);
        AtomicInteger cronLocks = new AtomicInteger(0);
        AtomicInteger crons = new AtomicInteger(0);
        AtomicLong lastCron = new AtomicLong(0);
        List<MorphiumId> processedIds = new Vector<>();
        AtomicInteger busyClients = new AtomicInteger();
        int cronInterval = 10000;
        GlobalLock gl = new GlobalLock();
        gl.scope = "global";
        morphium.insert(gl);
        GlobalLock cronLock = new GlobalLock();
        cronLock.scope = "Cron";
        morphium.insert(cronLock);

        try {
            var msgL = new MessageListener<Msg>() {
                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    try {
                        // log.info(msg.getSenderId() + ": OnMessage " + m.getName());
                        var cnt = morphium.createQueryFor(MessageCount.class).f("name").eq(m.getName()).countAll();

                        if (maxParallel.containsKey(m.getName()) && maxParallel.get(m.getName()) != null) {
                            if (cnt > maxParallel.get(m.getName())) { //has to be bigger, as the entry is already there for _this_ execution!
                                // log.error(String.format("MaxParallel for %s exceeded in ONMESSAGE! - pausing!", m.getName()));
                                msg.pauseProcessingOfMessagesNamed(m.getName());
                                morphium.createQueryFor(MessageCount.class).f("_id").eq(m.getMsgId()).delete();
                                throw new MessageRejectedException("MaxParallel exceeded", true);
                            }
                        }

                        busyClients.incrementAndGet();

                        //no maxParallel - can do our magic
                        if (processedIds.contains(m.getMsgId())) {
                            log.error("Duplicate Message processing for ID " + m.getMsgId());
                        }

                        processedIds.add(m.getMsgId());
                        //simulate to process message
                        counts.incrementAndGet();
                        Thread.sleep(((int)(Math.random() * 500.0) + 100));
                        //reset message count
                        busyClients.decrementAndGet();
                        var ret = morphium.createQueryFor(MessageCount.class).f("_id").eq(m.getMsgId()).delete();
                        // log.info("Deleted: " + Utils.toJsonString(ret));
                    } catch (InterruptedException e) {
                        log.info("Onmessage aborting");
                        // e.printStackTrace();
                    }

                    // log.info(msg.getSenderId() + ": onMessage finished - " + m.getName());
                    return null;
                }
            };
            final int noOfClients = 25;
            GlobalLock l = morphium.createQueryFor(GlobalLock.class).f("_id").eq("global").get();
            GlobalLock cl = morphium.createQueryFor(GlobalLock.class).f("_id").eq("Cron").get();

            for (int i = 0; i < noOfClients; i++) {
                log.info("Creating client " + i);
                Messaging rec1 = new Messaging(morphium, 100, true, true, 1);
                rec1.setSenderId("rec" + i);
                rec1.addListenerForMessageNamed("msg1", msgL);
                rec1.addListenerForMessageNamed("msg2", msgL);
                rec1.addListenerForMessageNamed("msg3", msgL);
                rec1.addListenerForMessageNamed("msg4", msgL);
                rec1.addListenerForMessageNamed("msg5", msgL);
                rec1.start();
                //   ____
                //  / ___|_ __ ___  _ __
                // | |   | '__/ _ \| '_ \
                // | |___| | | (_) | | | |
                //  \____|_|  \___/|_| |_|
                new Thread(()->{
                    while (!rec1.isRunning()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    }

                    while (rec1.isRunning()) {
                        //  if (morphium.createQueryFor(Msg.class).countAll() != 0 && morphium.createQueryFor(MessageCount.class).countAll() == 0) {
                        //      log.warn("There are queued messages, but not being processed.");
                        //      rec1.triggerCheck();
                        // }
                        if (System.currentTimeMillis() - lastCron.get() > cronInterval) {
                            //getting lock
                            try {
                                GlobalLock crLock = morphium.lockEntity(cl, rec1.getSenderId());

                                if (crLock != null && (crLock.lockedBy == null || !crLock.lockedBy.equals(rec1.getSenderId()))) {
                                    log.error("HELL IS LOOSE - lock does not work!");
                                    return;
                                }

                                if (crLock != null && crLock.lockedBy.equals(rec1.getSenderId())) {
                                    cronLocks.incrementAndGet();

                                    if (cronLocks.get() > 1) {
                                        log.error("!!!! TOO MANY LOCKS ON CRON!");
                                        cronLocks.decrementAndGet();
                                        morphium.releaseLock(crLock);
                                        continue;
                                    }

                                    log.info("Cron interval triggered...");
                                    Msg m = new Msg("msg2", "cron", "value");
                                    m.setDeleteAfterProcessing(true);
                                    m.setDeleteAfterProcessingTime(0);
                                    m.setLockedBy("Planner");
                                    m.setTimingOut(false);
                                    m.setLocked(System.currentTimeMillis());
                                    sender.sendMessage(m); //need to have a different sender ID than myself - or I won't be able to run this task
                                    crons.incrementAndGet();
                                    cronLocks.decrementAndGet();
                                    lastCron.set(System.currentTimeMillis());
                                    morphium.releaseLock(crLock);
                                }
                            } catch (Exception e) {
                                log.error("Something wrong in cron thread", e);
                            }
                        }

                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                        }
                    }
                }).start();
                ////////////////////////////////////////////////////////////////////////////////
                ///////////////////////////////
                //  ____  _
                // |  _ \| | __ _ _ __  _ __   ___ _ __
                // | |_) | |/ _` | '_ \| '_ \ / _ \ '__|
                // |  __/| | (_| | | | | | | |  __/ |
                // |_|   |_|\__,_|_| |_|_| |_|\___|_|
                Runnable plan = ()->{

                    try {
                        var running = morphium.createQueryFor(MessageCount.class).countAll();
                        // log.info(String.format("NoOfClients %d - running %d", noOfClients, running));

                        if (noOfClients - running > 0) {
                            var lock = morphium.lockEntity(l, rec1.getSenderId());

                            if (lock != null && (lock.lockedBy == null || !lock.lockedBy.equals(rec1.getSenderId()))) {
                                log.error("HELL IS LOOSE - lock does not work!");
                                return;
                            }

                            if (lock != null) { lock = morphium.reread(lock); }

                            if (lock != null && rec1.getSenderId().equals(lock.lockedBy)) {
                                //plan ahead
                                // log.info("Planner: Got lock!");
                                locks.incrementAndGet();

                                if (locks.get() > 1) {
                                    log.error(rec1.getSenderId() + ": Too many locks!!!!! " + locks.get());
                                    locks.decrementAndGet();
                                    morphium.releaseLock(lock);
                                    return;
                                }

                                var msgQuery = morphium.createQueryFor(Msg.class).f("locked_by").eq("Planner")
                                 .f("name").nin(rec1.getPausedMessageNames())
                                 .sort(Msg.Fields.priority, Msg.Fields.timestamp)
                                 .addProjection("_id").addProjection("name")
                                 .limit((int)(noOfClients - running));
                                var lst = msgQuery.asList();
                                // log.info(String.format("%s: Planner: Got %s candidates", rec1.getSenderId(), lst.size()));

                                if (lst.size() != 0) {
                                    // log.info(String.format("Planner: Processing %d messages", lst.size()));
                                    for (String pn : rec1.getPausedMessageNames()) {
                                        log.info("    " + pn);
                                    }

                                    for (var m : lst) {
                                        if (m.getLockedBy().equals("Planner")) {
                                            MessageCount msgCount = new MessageCount();
                                            msgCount.id = m.getMsgId().toString();
                                            msgCount.name = m.getName();
                                            morphium.store(msgCount);
                                            m.setLockedBy(null);

                                            try {
                                                morphium.updateUsingFields(m,  "locked_by");
                                            } catch (Exception e) {
                                                log.error("Planner: update failed!", e);
                                            }
                                        }
                                    }
                                }

                                locks.decrementAndGet();
                                morphium.releaseLock(lock);
                            }

                            // log.info("Planner: release lock");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
                new Thread() {
                    public void run() {
                        morphium.watch(MessageCount.class, true, new ChangeStreamListener() {
                            /////////////////////////////
                            //      _       _      ____                  _ __        __    _       _
                            //     | | ___ | |__  / ___|___  _   _ _ __ | |\ \      / /_ _| |_ ___| |__
                            //  _  | |/ _ \| '_ \| |   / _ \| | | | '_ \| __\ \ /\ / / _` | __/ __| '_ \
                            // | |_| | (_) | |_) | |__| (_) | |_| | | | | |_ \ V  V / (_| | || (__| | | |
                            //  \___/ \___/|_.__/ \____\___/ \__,_|_| |_|\__| \_/\_/ \__,_|\__\___|_| |_|
                            @Override
                            public boolean incomingData(ChangeStreamEvent evt) {
                                if (evt.getOperationType().equals("delete")) {
                                    // log.info("got delete! currently running: " + morphium.createQueryFor(MessageCount.class).countAll());
                                    for (String name : rec1.getPausedMessageNames()) {
                                        var count = morphium.createQueryFor(MessageCount.class).f("name").eq(name).countAll();

                                        if (!maxParallel.containsKey(name)) {
                                            // log.info("unpausing - no maxParallel set for " + name);
                                            rec1.unpauseProcessingOfMessagesNamed(name);
                                        } else if (count <= maxParallel.get(name)) {
                                            // log.info("unpausing - current running " + count + " MaxParallel is:" + maxParallel.get(name));
                                            rec1.unpauseProcessingOfMessagesNamed(name);
                                        }
                                    }

                                    synchronized (plan) {
                                        plan.run();
                                    }
                                } else if (evt.getOperationType().equals("insert")) {
                                    // log.info("got insert");
                                    // Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());
                                    rec1.triggerCheck();
                                }

                                return rec1.isRunning();
                            }
                        });
                    }
                }
                .start();
                new Thread() {
                    public void run() {
                        morphium.watch(Msg.class, true, new ChangeStreamListener() {
                            //  __  __         __        __    _       _
                            // |  \/  |___  __ \ \      / /_ _| |_ ___| |__
                            // | |\/| / __|/ _` \ \ /\ / / _` | __/ __| '_ \
                            // | |  | \__ \ (_| |\ V  V / (_| | || (__| | | |
                            // |_|  |_|___/\__, | \_/\_/ \__,_|\__\___|_| |_|
                            //             |___/
                            @Override
                            public boolean incomingData(ChangeStreamEvent evt) {
                                if (evt.getOperationType().equals("insert")) {
                                    // log.info("new Message!");
                                    Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());

                                    if (obj.getLockedBy() != null && obj.getLockedBy().equals("Planner")) {
                                        if (rec1.getPausedMessageNames().contains(obj.getName())) { return rec1.isRunning();}

                                        synchronized (plan) {
                                            plan.run();
                                        }
                                    }
                                }

                                return rec1.isRunning();
                            }
                        });
                    }
                }
                .start();
                clients.add(rec1);
            }

            //sending some messages
            int amount = 585;
            new Thread(()->{
                for (int i = 0; i < amount; i++) {
                    var mod = (i % 5) + 1;

                    if (mod == 1) {
                        // too much of them
                        if (Math.random() > 0.25) {
                            mod = 2; // no limit
                        }
                    }

                    if (mod == 5) {
                        if (Math.random() > 0.5) {
                            mod = 2;
                        }
                    }

                    Msg m = new Msg("msg" + mod, "msg", "value");
                    m.setDeleteAfterProcessing(true);
                    m.setDeleteAfterProcessingTime(0);
                    m.setLockedBy("Planner");
                    m.setTimingOut(false);
                    m.setLocked(System.currentTimeMillis());
                    assertTrue(m.isExclusive());
                    sender.sendMessage(m);
                    // log.info("Inserted " + m.getMsgId());
                }
            }).start();
            TestUtils.waitForConditionToBecomeTrue(160000, "Not all processed?", ()->(counts.get()) >= (amount + crons.get()), ()->{
                log.info("Waiting for " + counts.get() + " to reach " + (amount + crons.get()));
                log.info("  Active clients:" + busyClients.get());
                log.info("  Message Queue: " + morphium.createQueryFor(Msg.class).countAll());
                log.info("  processing   : " + morphium.createQueryFor(MessageCount.class).countAll());
                var agg = morphium.createAggregator(MessageCount.class, Map.class).group("$name").sum("count", 1).end().aggregateMap();

                for (var e : agg) {
                    if (maxParallel.get(e.get("_id")) != null) {
                        Integer maxp = (Integer)maxParallel.get(e.get("_id"));
                        Integer current = (Integer)e.get("count");

                        if (current > maxp) {
                            log.info("     " + e.get("_id") + " = " + e.get("count") + "(-1?)  max: " + maxParallel.get(e.get("_id")));
                        } else if (current > maxp + 1) {
                            log.error("ERROR--->     " + e.get("_id") + " = " + e.get("count") + "  max: " + maxParallel.get(e.get("_id")));
                        } else {
                            log.info("     " + e.get("_id") + " = " + e.get("count") + "  max: " + maxParallel.get(e.get("_id")));
                        }
                    }
                }
                log.info("------------------------------");
            });
            log.info(String.format("Messages planned: %d - processed: %d - cronjobs: %d", amount, counts.get(), crons.get()));
        } finally {
            for (Messaging m : clients) { m.terminate(); }

            sender.terminate();
        }
    }

    @Lockable
    @Entity
    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    public static class GlobalLock {
        @Id
        public String scope;
        @LockedBy
        public String lockedBy;
        @LockedAt
        public long lockedAt;
    }

    @Entity
    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    @Index("id,name")
    public static class MessageCount {
        @Id
        public String id;
        @Index
        public String name;
        @Index(options = "expireAfterSeconds:0")
        public Date delAt;
    }
}
