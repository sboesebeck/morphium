package de.caluga.test.mongo.suite;

import de.caluga.morphium.MongoDbMode;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Query;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.secure.DefaultSecurityManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import javax.swing.*;

/**
 * User: Stephan Bösebeck
 * Date: 09.08.12
 * Time: 09:35
 * <p/>
 * TODO: Add documentation here
 */
public class FailoverTests {
    private static Logger log = Logger.getLogger(FailoverTests.class);
    private static int writeError = 0;
    private static int readError = 0;
    private static int writes = 0;
    private boolean read = true;

    public MorphiumConfig getCfg() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig("morphium_test", MongoDbMode.REPLICASET, 5, 50000, 5000, new DefaultSecurityManager(), "morphium-log4j-test.xml");
        cfg.addAddress("localhost", 27017);
        cfg.addAddress("localhost", 27018);
        cfg.addAddress("localhost", 27019);
        cfg.setWriteCacheTimeout(100);
        cfg.setSlaveOk(true);
        return cfg;
    }

    @Test
    public void failoverTest() throws Exception {
        if (!System.getProperty("failovertest", "false").equals("true")) {
            log.info("Not running Failover test here");
            return;
        }
        Morphium morphium = null;
        try {
            morphium = new Morphium(getCfg());
        } catch (Exception e) {
            log.warn("Failovertest not possible?");
            return;
        }
        morphium.clearCollection(UncachedObject.class);

        //Writer-Thread
        WriterThread wt = new WriterThread(morphium);
        wt.start();
        Thread.sleep(1000); //let him write something
        new Thread() {
            public void run() {
                JOptionPane.showMessageDialog(null, "Stop");
                read = false;
            }
        }.start();
        while (read) {
            try {
                Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
                q.f("counter").lt(10000);
                long cnt = q.countAll();
                log.info("reading..." + cnt);
                assert (cnt > 0);
                log.info("Write Errors now: " + writeError);
                Thread.sleep(1000);
            } catch (Exception e) {
                log.error("Error during read", e);
                readError++;
            }
        }
        wt.setRunning(false);
        Thread.sleep(1000);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").lt(10000);
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
        private boolean running = true;
        private int i = 0;
        private Morphium morphium;

        public WriterThread(Morphium m) {
            morphium = m;
        }

        public void setRunning(boolean running) {
            this.running = running;
        }

        public void run() {
            while (running) {
                try {
                    UncachedObject o = new UncachedObject();
                    o.setValue("Value " + System.currentTimeMillis());
                    o.setCounter(i++);
                    writes++;
                    morphium.store(o);
                    log.info("Wrote. " + writes);

                } catch (Exception e) {
                    log.error("Error during storage", e);
                    writeError++;
                }

                try {
                    sleep(200);
                } catch (InterruptedException e) {

                }
            }
        }
    }

}
