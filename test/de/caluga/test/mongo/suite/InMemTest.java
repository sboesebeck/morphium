package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class InMemTest {
    public static Morphium morphium;


    private Logger log = LoggerFactory.getLogger(InMemTest.class);

    @org.junit.BeforeClass
    public static void setUpClass() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.addHostToSeed("inMem");
        cfg.setDatabase("test");
        cfg.setDriverClass(InMemoryDriver.class.getName());
        cfg.setReplicasetMonitoring(false);
        morphium = new Morphium(cfg);
    }

    @org.junit.After
    public void tearDown() {
        log.info("Cleaning up...");
        try {
            morphium.getDriver().drop("test", null);
        } catch (MorphiumDriverException e) {
            e.printStackTrace();
        }
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
