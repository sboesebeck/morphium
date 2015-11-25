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
