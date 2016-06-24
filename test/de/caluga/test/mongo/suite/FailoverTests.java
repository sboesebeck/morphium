package de.caluga.test.mongo.suite;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import javax.swing.*;

/**
 * User: Stephan Bösebeck
 * Date: 09.08.12
 * Time: 09:35
 * <p/>
 */
public class FailoverTests extends MongoTest {
    private static Logger log = new Logger(FailoverTests.class);
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
        new Thread() {
            @Override
            public void run() {
                JOptionPane.showMessageDialog(null, "Stop");
                read = false;
            }
        }.start();

        //create more readers

        for (int i = 0; i < readers; i++) {
            new Thread() {
                @Override
                public void run() {
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
                }
            }.start();
        }

        new Thread() {
            @Override
            public void run() {
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
            }
        }.start();

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

}
