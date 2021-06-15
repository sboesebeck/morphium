package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.inmem.InMemAggregator;
import de.caluga.morphium.driver.inmem.InMemAggregatorFactory;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.replicaset.OplogMonitor;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
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

    @org.junit.Before
    public void setup() {
        System.gc();
        log.info("creating in Memory instance");
        Properties p = MorphiumTestBase.getProps();
        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        cfg.setHostSeed("inMem");
        cfg.setDatabase("test");
        cfg.setDriverClass(InMemoryDriver.class.getName());
        cfg.setReplicasetMonitoring(false);
        cfg.setAggregatorFactory(new InMemAggregatorFactory());
        cfg.setAggregatorClass(InMemAggregator.class);
        morphium = new Morphium(cfg);
        log.info("Done!");
    }

    @org.junit.After
    public void tearDown() throws InterruptedException {
        log.info("Cleaning up...");

        try {
            Field f = morphium.getClass().getDeclaredField("shutDownListeners");
            f.setAccessible(true);
            List<ShutdownListener> listeners = (List<ShutdownListener>) f.get(morphium);
            for (ShutdownListener l : listeners) {
                if (l instanceof Messaging) {
                    ((Messaging) l).terminate();
                    log.info("Terminating still running messaging..." + ((Messaging) l).getSenderId());
                    while (((Messaging) l).isRunning()) {
                        log.info("Waiting for messaging to finish");
                        Thread.sleep(100);
                    }
                } else if (l instanceof OplogMonitor) {
                    ((OplogMonitor) l).stop();
                    while (((OplogMonitor) l).isRunning()) {
                        log.info("Waiting for oplogmonitor to finish");
                        Thread.sleep(100);
                    }
                    f = l.getClass().getDeclaredField("listeners");
                    f.setAccessible(true);
                    ((Collection) f.get(l)).clear();
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


    public boolean waitForAsyncOperationToStart(int maxWaits) {
        int cnt = 0;
        while (morphium.getWriteBufferCount() == 0) {
            Thread.yield();
            if (cnt++ > maxWaits) {
                return false;
            }
        }
        return true;
    }

    public void waitForWrites() {
        waitForWrites(morphium);
    }

    public void waitForWrites(Morphium morphium) {
        int count = 0;
        while (morphium.getWriteBufferCount() > 0) {
            count++;
            if (count % 100 == 0) {
                log.info("still " + morphium.getWriteBufferCount() + " writers active (" + morphium.getBufferedWriterBufferCount() + " + " + morphium.getWriterBufferCount() + ")");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }
        //waiting for it to be persisted
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
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
            uc.setValue("v");
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
