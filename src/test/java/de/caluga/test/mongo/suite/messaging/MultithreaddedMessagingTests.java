package de.caluga.test.mongo.suite.messaging;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.data.UncachedObject;



@Disabled
public class MultithreaddedMessagingTests extends MorphiumTestBase {

    @Test
    public void messagingMultithreaddedWriteTest() throws Exception {
        final Messaging sender = new Messaging(morphium);
        final int numThreads = 30;
        final List<Thread> threads = new ArrayList<>();
        final AtomicInteger count=new AtomicInteger();
        for (int t = 0; t < numThreads; t++) {
            String id = "thr" + t;
            var thr = new Thread() {
                public void run() {
                    for (int i = 0; i < 4000; i++) {
                        UncachedObject uc = new UncachedObject();
                        uc.setCounter(i);
                        uc.setStrValue("{ name: " + id+ ", counter:" + i + "}");
                        morphium.store(uc);
                        count.incrementAndGet();
                    }
                    threads.remove(this);
                }
            };
            threads.add(thr);
            thr.start();
        }
        log.info("Waiting for writers....");
        while (threads.size()!=0) {
           log.info("Threads active: "+threads.size()+" Messages: "+count.get());
            Thread.sleep(500);
        }
        log.info("done");
    }

    @Test
    public void messagingSendReceiveThreaddedTest() throws Exception {
        AtomicInteger procCounter = new AtomicInteger(0);
        final Messaging producer = new Messaging(morphium, 100, true, false, 10);
        final Messaging consumer = new Messaging(morphium, 100, true, true, 100);
        producer.start();
        consumer.start();
        Thread.sleep(2000);
        try {
            Vector<String> processedIds = new Vector<>();
            Hashtable<String,Long> processedAt=new Hashtable<>();
            procCounter.set(0);
            consumer.addMessageListener((msg, m) -> {
                procCounter.incrementAndGet();

                if (processedIds.contains(m.getMsgId().toString())) {
                    log.error("Received msg twice: " + procCounter.get() + "/" + m.getMsgId()+"  - "+(System.currentTimeMillis()-processedAt.get(m.getMsgId().toString()))+"ms ago");
                    return null;
                }
                processedIds.add(m.getMsgId().toString());
                processedAt.put(m.getMsgId().toString(),System.currentTimeMillis());
                //simulate processing
                try {
                    Thread.sleep((long)(100 * Math.random()));
                } catch (InterruptedException e) {
                }
                return null;
            });
            Thread.sleep(2500);
            int amount = 1000;
            log.info("------------- sending messages");

            for (int i = 0; i < amount; i++) {
                if (i%100==0){
                    log.info("Sending message "+i);
                }
                producer.sendMessage(new Msg("Test " + i, "msg " + i, "value " + i,30000,false));
            }
            log.info("--- sending finished");
            for (int i = 0; i < 30 && procCounter.get() < amount; i++) {
                Thread.sleep(1000);
                log.info("Still processing: " + procCounter.get());
            }
            assert(procCounter.get() == amount) : "Did process " + procCounter.get();
        } finally {
            producer.terminate();
            consumer.terminate();
        }
    }

    @Test
    public void mutlithreaddedMessagingPerformanceTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true, true, 2000);
        consumer.start();
        producer.start();
        Thread.sleep(2500);

        try {
            final AtomicInteger processed = new AtomicInteger();
            final Map<String, AtomicInteger> msgCountById = new ConcurrentHashMap<>();
            consumer.addMessageListener((msg, m) -> {
                processed.incrementAndGet();

                if (processed.get() % 1000 == 0) {
                    log.info("Consumed " + processed.get());
                }
                assert(!msgCountById.containsKey(m.getMsgId().toString()));
                msgCountById.putIfAbsent(m.getMsgId().toString(), new AtomicInteger());
                msgCountById.get(m.getMsgId().toString()).incrementAndGet();
                //simulate processing
                try {
                    Thread.sleep((long)(10 * Math.random()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return null;
            });
            int numberOfMessages = 1000;

            for (int i = 0; i < numberOfMessages; i++) {
                Msg m = new Msg("msg", "m", "v");
                m.setTtl(5 * 60 * 1000);

                if (i % 1000 == 0) {
                    log.info("created msg " + i + " / " + numberOfMessages);
                }

                producer.sendMessage(m);
            }

            long start = System.currentTimeMillis();

            while (processed.get() < numberOfMessages) {
                //            ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
                //            log.info("Running threads: " + thbean.getThreadCount());
                log.info("Processed " + processed.get());
                Thread.sleep(1500);
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Processing took " + dur + " ms");
            assert(processed.get() == numberOfMessages);

            for (String id : msgCountById.keySet()) {
                assert(msgCountById.get(id).get() == 1);
            }
        } finally {
            producer.terminate();
            consumer.terminate();
        }
    }

    @Test
    public void waitingForMessagesIfNonMultithreadded() throws Exception {
        final List<Msg> list = new ArrayList<>();
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, false, 10);
        sender.start();
        list.clear();
        Messaging receiver = new Messaging(morphium, 100, false, false, 10);
        receiver.addMessageListener((msg, m) -> {
            list.add(m);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            return null;
        });
        receiver.start();

        try {
            Thread.sleep(500);
            sender.sendMessage(new Msg("test", "test", "test"));
            sender.sendMessage(new Msg("test", "test", "test"));

            while (list.size() == 0) {
                Thread.sleep(500);
            }

            assert(list.size() == 1) : "Size wrong: " + list.size();
            Thread.sleep(2200);
            assert(list.size() == 2);
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @Test
    public void waitingForMessagesIfMultithreadded() throws Exception {
        final List<Msg> list = new ArrayList<>();
        morphium.dropCollection(Msg.class);
        morphium.getConfig().setThreadPoolMessagingCoreSize(5);
        log.info("Max threadpool:" + morphium.getConfig().getThreadPoolMessagingCoreSize());
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        list.clear();
        Messaging receiver = new Messaging(morphium, 100, false, true, 10);
        receiver.addMessageListener((msg, m) -> {
            log.info("Incoming message...");
            list.add(m);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            return null;
        });
        receiver.start();

        try {
            Thread.sleep(500);
            sender.sendMessage(new Msg("test", "test", "test"));
            sender.sendMessage(new Msg("test", "test", "test"));

            while (list.size() < 2) {
                Thread.sleep(100);
            }

            Thread.sleep(100);
            assert(list.size() == 2) : "Size wrong: " + list.size();
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @Test
    public void multithreaddingTestSingle() throws Exception {
        int amount = 65;
        Messaging producer = new Messaging(morphium, 500, false);
        producer.start();

        for (int i = 0; i < amount; i++) {
            if (i % 10 == 0) {
                log.info("Messages sent: " + i);
            }

            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 30000);
            producer.sendMessage(m);
        }

        final AtomicInteger count = new AtomicInteger();
        Messaging consumer = new Messaging(morphium, 100, false, true, 1000);
        consumer.addMessageListener((msg, m) -> {
            //            log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.start();

        while (count.get() < amount) {
            log.info("Messages processed: " + count.get());
            Thread.sleep(1000);

            if (System.currentTimeMillis() - start > 20000) {
                throw new RuntimeException("Timeout");
            }
        }

        long dur = System.currentTimeMillis() - start;
        log.info("processing " + amount + " multithreaded but single messages took " + dur + "ms == " + (amount / (dur / 1000)) + " msg/sec");
        consumer.terminate();
        producer.terminate();
    }

    @Test
    public void multithreaddingTestMultiple() throws Exception {
        int amount = 650;
        Messaging producer = new Messaging(morphium, 500, false);
        producer.start();
        log.info("now multithreadded and multiprocessing");

        for (int i = 0; i < amount; i++) {
            if (i % 10 == 0) {
                log.info("Messages sent: " + i + "/" + amount);
            }

            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 300000);
            producer.sendMessage(m);
        }

        final AtomicInteger count = new AtomicInteger();
        count.set(0);
        Messaging consumer = new Messaging(morphium, 100, true, true, 121);
        consumer.addMessageListener((msg, m) -> {
            //log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.start();

        while (count.get() < amount) {
            log.info("Messages processed: " + count.get() + "/" + amount);
            Thread.sleep(1000);

            if (System.currentTimeMillis() - start > 20000) {
                throw new RuntimeException("Timeout!");
            }

            //            for (var e:morphium.getDriver().getDriverStats().entrySet()){
            //                log.info("Stats: "+e.getKey()+" - " + e.getValue());
            //            }
            var stats = morphium.getDriver().getDriverStats();
            log.info("Connections in Pool : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL));
            log.info("Connections opened  : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_OPENED));
            log.info("Connections borrowed: " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED));
            log.info("Connections released: " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED));
            log.info("Connections in use  : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_USE));
            log.info("-------------------------------------");
        }

        long dur = System.currentTimeMillis() - start;
        log.info("processing " + amount + " multithreaded and multiprocessing messages took " + dur + "ms == " + (amount / (dur / 1000)) + " msg/sec");
        consumer.terminate();
        producer.terminate();
        log.info("Messages processed: " + count.get());
        log.info("Messages left: " + consumer.getPendingMessagesCount());
    }

}
