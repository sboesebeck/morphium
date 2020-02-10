package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Stephan Bösebeck
 * Date: 09.08.12
 * Time: 09:35
 * <p/>
 */
public class FailoverTests extends MorphiumTestBase {
    private static Logger log = LoggerFactory.getLogger(FailoverTests.class);
    private static int writeError = 0;
    private static int readError = 0;
    private static int writes = 0;
    private boolean read = true;


    @Test
    public void failoverTest() throws Exception {
        if (!getProps().getProperty("failovertest", "false").equals("true")) {
            log.info("Not running Failover test here");
            return;
        }
        int readers = 150;
        int writers = 100;


        morphium.clearCollection(UncachedObject.class);

        //Writer-Thread

        //create a couple of writers
        for (int i = 0; i < writers; i++) {
            WriterThread wt = new WriterThread(morphium);
            wt.start();
        }
        Thread.sleep(1000); //let him write something
        new Thread(() -> {
            JOptionPane.showMessageDialog(null, "Stop");
            read = false;
        }).start();

        //create more readers

        for (int i = 0; i < readers; i++) {
            new Thread(() -> {
                while (read) {
                    try {
                        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
                        long cnt = q.asList().size();
                        //                            log.info("reading..." + cnt);
                        assert (cnt > 0);
                        Thread.sleep((long) (1000 * Math.random() + 100));
                    } catch (Exception e) {
                        log.error("Error during read", e);
                        readError++;
                    }
                }
            }).start();
        }

        new Thread(() -> {
            while (read) {
                Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
                //        q.f("counter").lt(10000);
                long cnt = q.countAll();
                log.info("Last count       : " + cnt);
                log.info("Number of writes : " + writes);
                log.info("Write errors     : " + writeError);
                log.info("Read errors      : " + readError);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }
            }
        }).start();

        while (read) {
            Thread.sleep(1000);
        }

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        //        q.f("counter").lt(10000);
        long cnt = q.countAll();
        log.info("Last count       : " + cnt);
        log.info("Number of writes : " + writes);
        log.info("Write errors     : " + writeError);
        log.info("Read errors      : " + readError);
        //        assert(cnt==writes):"Writes wrong: "+writes+" vs count: "+cnt;
        //        assert(writeError<5):"Write errors: "+writeError;
        log.info("Done test");
    }


    @Embedded
    public class WriterThread extends Thread {
        private int i = (int) (Math.random() * 10000);
        private Morphium morphium;

        public WriterThread(Morphium m) {
            morphium = m;
        }


        @Override
        public void run() {
            while (read) {
                try {
                    UncachedObject o = new UncachedObject();
                    o.setValue("Value " + System.currentTimeMillis());
                    o.setCounter(i++);
                    writes++;
                    morphium.store(o);
                    //                    log.info("Wrote. " + writes);

                } catch (Exception e) {
                    log.error("Error during storage", e);
                    writeError++;
                }

                try {
                    sleep((long) (200 + Math.random() * 500));
                } catch (InterruptedException e) {

                }
            }
        }
    }


    @Test
    public void failoverMessagingTest() throws Exception {
//        if (!getProps().getProperty("failovertest", "false").equals("true")) {
//            log.info("Not running Failover test here");
//            return;
//        }
        Messaging sender = new Messaging(morphium, 500, false);
        sender.setSenderId("sender");
        sender.start();
        Messaging receiver = new Messaging(morphium, 500, false);
        receiver.setSenderId("receiver");
        receiver.start();

        final AtomicInteger sent = new AtomicInteger();
        final AtomicInteger rec = new AtomicInteger();

        receiver.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
//                log.info("Got msg: "+m.getMsgId().toString());
                rec.incrementAndGet();
                return null;
            }
        });

        while (true) {
            sender.sendMessage(new Msg("test", "test", "test"));
            sent.incrementAndGet();

            Thread.sleep(1000);
            log.info("Sent: " + sent.get() + "  received: " + rec.get());
        }
    }

}
