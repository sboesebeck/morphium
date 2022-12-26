package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.DefaultReadPreference;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class SingleCollectionJobQueue extends MorphiumTestBase {
    @Test
    public void rejectPauseReplayTest() throws Exception {
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Vector<Messaging> clients = new Vector<>();
        Map<String, Integer> maxParallel = Map.of("msg1", 1, "msg3", 15, "msg4", 29, "msg5", 3);
        AtomicInteger counts = new AtomicInteger(0);
        AtomicInteger locks = new AtomicInteger(0);
        AtomicInteger cronLocks = new AtomicInteger(0);
        AtomicInteger crons = new AtomicInteger(0);
        AtomicLong lastCron = new AtomicLong(0);
        AtomicInteger errors = new AtomicInteger(0);
        List<MorphiumId> processedIds = new Vector<>();
        Map<MorphiumId,List<Object>> processedIdsMetaInfo=new ConcurrentHashMap<>();
        List<String> lockedBy = new Vector<>();
        AtomicInteger busyClients = new AtomicInteger();
        int cronInterval = 10000;
        GlobalLock gl = new GlobalLock();
        gl.scope = "global";
        morphium.insert(gl);
        GlobalLock cl = new GlobalLock();
        cl.scope = "cron";
        morphium.insert(cl);
        AtomicInteger onMessage = new AtomicInteger(0);

        try {
            var msgL = new MessageListener<Msg>() {
                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    onMessage.incrementAndGet();

                     MsgLock msglock=morphium.createQueryFor(MsgLock.class).setCollectionName("msg_lck").f("_id").eq(m.getMsgId()).get();                        // plan ahead
                    if (msglock==null){
                        log.error("Not locked?!?!?!"+m.isExclusive() + " -> "+m.getName());
                    } else {
                        if (!msglock.getLockId().equals(msg.getSenderId())){
                            log.error("Locked by someone else: "+msglock.getLockId());
                        }
                    }
                    try {
                        busyClients.incrementAndGet();
                        // log.info(msg.getSenderId() + ": OnMessage " + m.getName());
                        if (maxParallel.containsKey(m.getName()) && maxParallel.get(m.getName()) != null) {
                            var cnt = morphium.createQueryFor(Msg.class).f("name").eq(m.getName()).f("processed_by.0").eq(null).countAll();

                            if (cnt > maxParallel.get(m.getName()) + 1) { // has to be bigger, as the entry is already
                                // there
                                // for _this_ execution!
                                // log.error(String.format("MaxParallel for %s exceeded in ONMESSAGE! -
                                // pausing!", m.getName()));
                                msg.pauseProcessingOfMessagesNamed(m.getName());
                                var ex = new MessageRejectedException("MaxParallel exceeded", true);
                                ex.setCustomRejectionHandler((messaging, message)->{
                                    message.setProcessedBy(new ArrayList<>());
                                    morphium.store(message);
                                });
                                throw ex;
                            }
                        }

                        // if (m.getProcessedBy()!=null && m.getProcessedBy().size()>0){
                        //     log.error("Message was already processed?!?!?!?");
                        // }
                        // no maxParallel - can do our magic
                        if (processedIds.contains(m.getMsgId())) {
                            log.error(msg.getSenderId()+": Duplicate Message processing for ID " + m.getMsgId());
                            log.error(msg.getSenderId()+": Processed by: "+m.getProcessedBy().size());
                            for (String p:m.getProcessedBy()){
                                log.error("    Processed by: "+p);
                            }
                            List<Object> p=processedIdsMetaInfo.get(m.getMsgId());
                            log.error(String.format("  processed %d ms ago - id %s",(System.currentTimeMillis()-(long)p.get(0)),p.get(1)));
                            errors.incrementAndGet();
                        }

                        processedIds.add(m.getMsgId());
                        processedIdsMetaInfo.put(m.getMsgId(),Arrays.asList(System.currentTimeMillis(),msg.getSenderId()));
                        // simulate to process message
                        counts.incrementAndGet();
                        Thread.sleep(((int)(Math.random() * 500.0) + 1500));
                        // reset message count
                        busyClients.decrementAndGet();
                        // log.info("Deleted: " + Utils.toJsonString(ret));
                    } catch (InterruptedException e) {
                        log.info("Onmessage aborting");
                        // e.printStackTrace();
                    } finally {
                        // log.info(msg.getSenderId() + " onmessage finished");
                        onMessage.decrementAndGet();
                    }

                    // log.info(msg.getSenderId() + ": onMessage finished - " + m.getName());
                    return null;
                }
            };
            final int noOfClients = 25;

            for (int i = 0; i < noOfClients; i++) {
                log.info("Creating client " + i);
                Messaging rec1 = new Messaging(morphium, 100, true, true, 5);
                rec1.setSenderId("rec" + i);
                rec1.addListenerForMessageNamed("msg1", msgL);
                rec1.addListenerForMessageNamed("msg2", msgL);
                rec1.addListenerForMessageNamed("msg3", msgL);
                rec1.addListenerForMessageNamed("msg4", msgL);
                rec1.addListenerForMessageNamed("msg5", msgL);
                rec1.start();
                ///////////////////////////////
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
                        if (System.currentTimeMillis() - lastCron.get() > cronInterval) {
                            // getting lock
                            // log.info("Should trigger Cron...");
                            rec1.triggerCheck();
                            GlobalLock lock = new GlobalLock();
                            lock.scope = "Cron";

                            try {
                                morphium.insert(lock);
                            } catch (Exception e) {
                                //lock failed
                                return;
                            }

                            try {
                                cronLocks.incrementAndGet();

                                if (cronLocks.get() > 1) {
                                    log.error(rec1.getSenderId() + " CRON: Too many locks  " + cronLocks.get());
                                    errors.incrementAndGet();
                                }

                                // log.info("Cron interval triggered...");
                                Msg m = new Msg("msg2", "cron", "value");
                                m.setDeleteAfterProcessing(true);
                                m.setDeleteAfterProcessingTime(0);
                                m.setTimingOut(false);
                                m.setExclusive(true);
                                m.setProcessedBy(Arrays.asList("Planner"));
                                sender.sendMessage(m); // need to have a different sender ID than myself - or I
                                // won't be able to run this task
                                crons.incrementAndGet();
                                lastCron.set(System.currentTimeMillis());
                                cronLocks.decrementAndGet();
                                morphium.delete(lock);
                            } catch (Exception e) {
                                log.error("Something wrong in cron thread", e);
                                errors.incrementAndGet();
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
                    var running = morphium.createQueryFor(Msg.class).f("processed_by.0").eq(null).countAll();

                    if (noOfClients <= running) {
                        return;
                    }

                    var lock =  new GlobalLock();
                    lock.scope = "Planner";

                    try {
                        morphium.insert(lock);
                    } catch (Exception e) {
                        //lock failed
                        return;
                    }

                    try {
                        locks.incrementAndGet();

                        if (locks.get() > 1) {
                            log.error(rec1.getSenderId() + ": Too many locks for planner " + locks.get());
                            errors.incrementAndGet();
                        }

                        try {
                            // log.info(rec1.getSenderId() + " Planner: Got lock!");
                            var agg = morphium.createAggregator(Msg.class, Map.class)
                             .match(morphium.createQueryFor(Msg.class).f("processed_by.0").eq(null))
                             .group("$name").sum("count", 1).end().aggregateMap();

                            for (var e : agg) {
                                if (maxParallel.containsKey(e.get("_id"))) {
                                    if (rec1.getPausedMessageNames().contains(e.get("_id"))
                                     && ((Integer) e.get("count")) < maxParallel.get(e.get("_id"))) {
                                        rec1.unpauseProcessingOfMessagesNamed(e.get("_id").toString());
                                    } else if (maxParallel.get(e.get("_id")) >= (Integer) e.get("count")) {
                                        rec1.pauseProcessingOfMessagesNamed(e.get("_id").toString());
                                    }
                                }
                            }

                            var msgQuery = morphium.createQueryFor(Msg.class).f("processed_by").eq("Planner")
                             .f("name").nin(rec1.getPausedMessageNames())
                             .sort(Msg.Fields.priority, Msg.Fields.timestamp)
                             .addProjection("_id").addProjection("name")
                             .limit((int)(noOfClients - running));
                            var lst = msgQuery.asList();
                            // log.info(String.format("%s: Planner: Got %s candidates", rec1.getSenderId(), lst.size()));

                            if (lst.size() != 0) {
                                // log.info(String.format("Planner: Processing %d messages", lst.size()));
                                // for (String pn : rec1.getPausedMessageNames()) {
                                // log.info(" " + pn);
                                // }
                                int added = 0;

                                do {
                                    for (var m : lst) {
                                        if (maxParallel.containsKey(m.getName())) {
                                            var cnt = morphium.createQueryFor(Msg.class).f("name")
                                             .eq(m.getName()).f("processed_by.0").eq(null).countAll();

                                            if (cnt >= (Integer) maxParallel.get(m.getName())) {
                                                rec1.pauseProcessingOfMessagesNamed(m.getName());
                                                continue;
                                            }
                                        }

                                        added++;
                                        // m.setLockedBy(null);
                                        m.setProcessedBy(null);

                                        try {
                                            morphium.createQueryFor(Msg.class).f("_id").eq(m.getMsgId()).unset("processed_by");
                                        } catch (Exception e) {
                                            log.error("Planner: update failed!", e);
                                            m = morphium.reread(m);
                                            log.error("Planner: "
                                             + Utils.toJsonString(morphium.getMapper().serialize(m)));
                                            errors.incrementAndGet();
                                        }
                                        // }
                                    }

                                    if (added < (noOfClients - running)) {
                                        msgQuery = morphium.createQueryFor(Msg.class).f("processed_by").eq("Planner")
                                         .f("name").nin(rec1.getPausedMessageNames())
                                         .sort(Msg.Fields.priority, Msg.Fields.timestamp)
                                         .addProjection("_id").addProjection("name")
                                         .limit((int)(noOfClients - running - added));
                                        lst = msgQuery.asList();

                                        if (lst.isEmpty()) {
                                            break;
                                        }
                                    }
                                } while (added < (noOfClients - running));

                                // log.info(String.format("Planner added %d entries (Running: %d - available
                                // %d)!", added, running, noOfClients));
                            }
                        } finally {
                            // log.info("Planner: release lock");
                            lockedBy.remove(rec1.getSenderId());
                            locks.decrementAndGet();
                            morphium.delete(lock);
                            // log.info(String.format("Whole planning took %d ms", System.currentTimeMillis() - start));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
                //////////////////////////////////////////////////////
                //  __  __         __        __    _       _
                // |  \/  |___  __ \ \      / /_ _| |_ ___| |__
                // | |\/| / __|/ _` \ \ /\ / / _` | __/ __| '_ \
                // | |  | \__ \ (_| |\ V  V / (_| | || (__| | | |
                // |_|  |_|___/\__, | \_/\_/ \__,_|\__\___|_| |_|
                //             |___/
                new Thread() {
                    public void run() {
                        morphium.watch(Msg.class, true, new ChangeStreamListener() {
                            @Override
                            public boolean incomingData(ChangeStreamEvent evt) {
                                if (evt.getOperationType().equals("insert")) {
                                    // log.info("new Message!");
                                    Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());
                                    if (obj.getProcessedBy() != null && obj.getProcessedBy().contains("Planner")) {
                                        if (rec1.getPausedMessageNames().contains(obj.getName())) {
                                            return rec1.isRunning();
                                        }

                                        plan.run();
                                    }
                                } else if (evt.getOperationType().equals("delete")) {
                                    // log.info(rec1.getSenderId()+": incoming delete");
                                    for (String n : rec1.getPausedMessageNames()) {
                                        if (maxParallel.containsKey(n)) {
                                            if (morphium.createQueryFor(Msg.class).f("processed_by.0").eq(null).countAll() < maxParallel.get(n)) {
                                                long dur = rec1.unpauseProcessingOfMessagesNamed(n);

                                                // log.info(n + " was paused for " + dur + "ms");
                                                // if (dur > 15000) {
                                                //     log.warn(n + " was paused too long: " + dur);
                                                // }
                                            }
                                        } else {
                                            rec1.unpauseProcessingOfMessagesNamed(n);
                                        }
                                    }

                                    rec1.triggerCheck();
                                    // log.info(rec1.getSenderId() + ": calling planner because of Delete Msg");
                                    plan.run(); // plan ahead - there is a free slot!
                                }

                                return rec1.isRunning();
                            }
                        });
                    }
                }
                .start();
                clients.add(rec1);
            }

            // sending some messages
            int amount = 585;
            var block = new ArrayList<>();

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
                m.setTimingOut(false);
                m.setExclusive(true);
                m.setProcessedBy(Arrays.asList("Planner"));
                assertTrue(m.isExclusive());
                m.setSender(sender.getSenderId());
                m.setSenderHost("localhost");
                block.add(m);

                if (block.size() > 100) {
                    morphium.insert(block);
                    block.clear();
                }

                // log.info("Inserted " + m.getMsgId());
            }

            if (block.size() != 0) {
                morphium.insert(block);
                block.clear();
            }

            log.info("=========================> ALL MESSAGES SENT");
            long start = System.currentTimeMillis();
            int maxRunning = 0;
            int maxPlanned = 0;

            while (!(counts.get() >= (amount + crons.get()))) {
                if (System.currentTimeMillis() - start > 1000) {
                    Msg m = new Msg("morphium.status_info", "status", "value", 4000);
                    m.setPriority(10);
                    m.setTimingOut(false);
                    m.setDeleteAfterProcessing(true);
                    m.setDeleteAfterProcessingTime(0);
                    start = System.currentTimeMillis();
                    log.info("---------------------------------------------------------------------------------------------");
                    log.info("---------------->>>>>>> Waiting for " + counts.get() + " to reach "
                     + (amount + crons.get()));
                    var answers = sender.sendAndAwaitAnswers(m, 25, 15000, false);
                    log.info("  Got answers   : " + answers.size());
                    log.info("  onMessage par : " + onMessage.get());
                    log.info("  Active clients: " + busyClients.get() + " Max: " + maxRunning);
                    log.info("  Message Queue : " + morphium.createQueryFor(Msg.class).countAll());
                    log.info("  max scheduled : " + maxPlanned);
                    log.info("  processing    : " + morphium.createQueryFor(Msg.class).f("processed_by.0").eq(null).countAll());
                    var agg = morphium.createAggregator(Msg.class, Map.class)
                     .match(morphium.createQueryFor(Msg.class).f("processed_by.0").eq(null)).group("$name")
                     .sum("count", 1).end().sort("_id").aggregateMap();

                    for (var e : agg) {
                        if (maxParallel.get(e.get("_id")) != null) {
                            Integer maxp = (Integer) maxParallel.get(e.get("_id"));
                            Integer current = (Integer) e.get("count");

                            if (current > maxp) {
                                log.error(" ERROR --->     " + e.get("_id") + " = " + e.get("count") + "  max: "
                                 + maxParallel.get(e.get("_id")));
                                errors.incrementAndGet();
                            } else {
                                log.info("     " + e.get("_id") + " = " + e.get("count") + "  max: " + maxp);
                            }
                        }
                    }

                    agg = morphium.createAggregator(Msg.class, Map.class)
                     .match(morphium.createQueryFor(Msg.class).f("processed_by").eq("Planner")).group("$name")
                     .sum("count", 1).end().sort("_id").aggregateMap();
                    log.info("  Messages queued: ");

                    for (var e : agg) {
                        if (maxParallel.get(e.get("_id")) != null) {
                            log.info("     " + e.get("_id") + " = " + e.get("count"));
                        }
                    }

                    log.info(String.format("Messages planned: %d - processed: %d - cronjobs: %d", amount, counts.get(), crons.get()));
                    log.info("------------------------------");
                    assertEquals(0, errors.get(), "Errors occured: " + errors.get());
                    start = System.currentTimeMillis();
                }

                if (busyClients.get() > maxRunning) {
                    maxRunning = busyClients.get();
                }

                var scheduled = (int) morphium.createQueryFor(Msg.class).f("processed_by.0").eq(null).countAll();

                if (scheduled > maxPlanned) {
                    maxPlanned = scheduled;
                }

                Thread.sleep(10);
            }
        } finally {
            for (Messaging m : clients) {
                m.terminate();
            }

            sender.terminate();
        }
    }

    @Entity
    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    public static class GlobalLock {
        @Id
        public String scope;
    }
}
