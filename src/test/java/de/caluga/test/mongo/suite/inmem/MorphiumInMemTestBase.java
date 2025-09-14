package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class MorphiumInMemTestBase {
    public static Morphium morphium;


    public Logger log = LoggerFactory.getLogger(MorphiumInMemTestBase.class);

    @BeforeEach
    public void setup() {
        System.gc();
        log.info("creating in Memory instance");
        MorphiumConfig cfg = de.caluga.test.support.TestConfig.forDriver(InMemoryDriver.driverName);
        cfg.setHostSeed("inMem");
        cfg.setDatabase("test");
        cfg.setReplicasetMonitoring(false);
        cfg.setMaxWaitTime(1550);
        morphium = new Morphium(cfg);
        log.info("Done!");

    }

    @AfterEach
    public void tearDown() throws Exception {
        log.info("Cleaning up...");

        try {
            Field f = morphium.getClass().getDeclaredField("shutDownListeners");
            f.setAccessible(true);
            List<ShutdownListener> listeners = (List<ShutdownListener>) f.get(morphium);
            for (ShutdownListener l : listeners) {
                if (l instanceof MorphiumMessaging) {
                    MorphiumMessaging mm = (MorphiumMessaging) l;
                    mm.terminate();
                    log.info("Terminating still running messaging..." + mm.getSenderId());
                    long start=System.currentTimeMillis();
                    while (mm.isRunning()) {
                        log.info("Waiting for messaging to finish");
                        long dur=System.currentTimeMillis()-start;

                        if (dur>5000 && dur<5500){
                            log.warn("Shutting down failed, retrying");
                            mm.terminate();
                        }
                        if (dur>10000){
                            throw new RuntimeException("Could not terminate messaging!");
                        }
                        Thread.sleep(1000);
                    }
                } else if (l instanceof ChangeStreamMonitor) {
                    log.info("Changestream Monitor still running");
                    ((ChangeStreamMonitor) l).terminate();
                    while (((ChangeStreamMonitor) l).isRunning()) {
                        log.info("Waiting for changestreamMonitor to finish");
                        Thread.sleep(100);
                    }
                    f = l.getClass().getDeclaredField("listeners");
                    f.setAccessible(true);
                    ((Collection) f.get(l)).clear();
                } else if (l instanceof BufferedMorphiumWriterImpl) {
                    ((BufferedMorphiumWriterImpl) l).close();
                }
            }
        } catch (Exception e) {
            log.error("Could not shutdown properly!", e);
        }

        morphium.close();
        morphium = null;
        //Thread.sleep(1000);
        log.info("done...");
    }


    public boolean waitForWriteBufferToFlush(long maxWaitMs) {
        long start = System.currentTimeMillis();
        while (morphium.getWriteBufferCount() == 0) {
            Thread.yield();
            if (System.currentTimeMillis() - start > maxWaitMs) {
                return false;
            }
        }
        return true;
    }

    public void waitForWrites() {
        TestUtils.waitForWrites(morphium,log);
    }
    public void createCachedObjects(Morphium morphium, int amount) {
        List<CachedObject> lst = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            CachedObject uc = new CachedObject();
            uc.setCounter(i + 1);
            uc.setValue("v");
            lst.add(uc);
        }
        morphium.storeList(lst);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void createUncachedObjects(int amount) {
        createUncachedObjects(morphium, amount);
    }

    public void createUncachedObjects(Morphium morphium, int amount) {
        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setStrValue("v");
            lst.add(uc);
            if (i % 1000 == 999) {
                morphium.storeList(lst);
                lst.clear();
            }
        }
        morphium.storeList(lst);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        while (q.countAll() != amount) {
            log.info("Waiting for data to be stored..." + q.countAll());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }
}
