package de.caluga.test.mongo.suite.locking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.locking.Lockable;
import de.caluga.morphium.annotations.locking.LockedAt;
import de.caluga.morphium.annotations.locking.LockedBy;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class LockEntityTest extends MorphiumTestBase {

    @Test
    public void simpleLocking() throws Exception {
        morphium.dropCollection(LockedEntity.class);

        for (int i = 0; i < 100; i++) {
            LockedEntity le = new LockedEntity();
            le.value = i;
            morphium.store(le);
        }

        //locking
        var q = morphium.createQueryFor(LockedEntity.class).f("value").gt(42).limit(2);
        var res = morphium.lockEntities(q, "Its me", 1000);
        assertEquals(2, res.size(), "Not all locked! " + res.size());
        var l = q.asList();

        for (var le : l) {
            assertEquals(le.lockedBy, "Its me");
        }

        log.info("Done " + l.size());
    }

    @Test
    public void outdatedLocks() throws Exception {
        morphium.dropCollection(LockedEntity.class);

        for (int i = 0; i < 100; i++) {
            LockedEntity le = new LockedEntity();
            le.value = i;
            morphium.store(le);
        }

        //locking
        var q = morphium.createQueryFor(LockedEntity.class).f("value").lt(2).limit(2);
        var res = morphium.lockEntities(q, "Its me", 1000);
        assertEquals(2, res.size(), "Not all locked! " + res.size());
        Thread.sleep(1500);
        q = morphium.getOutdatedLocksQuery(LockedEntity.class, 1000);
        assertEquals(2, q.countAll());
        morphium.releaseLocksOutdated(LockedEntity.class, 1000);
        assertEquals(0, q.countAll());
        q = morphium.createQueryFor(LockedEntity.class).f("value").lt(2).limit(2);
        res = morphium.lockEntities(q, "Its me", 1000);
        assertEquals(2, q.countAll());
    }

    @Test
    public void exclusiveQueueTest() throws Exception {
        morphium.dropCollection(LockedEntity.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, LockedEntity.class);
        log.info("Storing objects...");
        final List<LockedEntity> lst = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            LockedEntity le = new LockedEntity();
            le.value = i;
            lst.add(le);
        }

        morphium.storeList(lst);
        log.info("done");
        Vector<Integer> values = new Vector<Integer>();

        for (int i = 0; i < 10; i++) {
            var t=i;
            Runnable r = ()-> {
                var q = morphium.createQueryFor(LockedEntity.class).sort("value").limit(1);

                try {
                    while (true) {
                        var res = morphium.lockEntities(q, "thr_"+t, 1000);
                        if (res == null || res.isEmpty()) { return; }
                        assertEquals(1,res.size());
                        if (values.contains(res.get(0).value)){
                            log.error("Error - duplicate entry!");
                        } else {
                            values.add(res.get(0).value);
                        }
                        morphium.delete(res.get(0));
                    }
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            };
            new Thread(r).start();
        }

        while (morphium.createQueryFor(LockedEntity.class).countAll()!=0){
            log.info("Waiting for queue to be processed..."+morphium.createQueryFor(LockedEntity.class).countAll());
            Thread.sleep(500);       
        }

    }


    @Test
    public void multithreaddedLocking() throws Exception {
        morphium.dropCollection(LockedEntity.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, LockedEntity.class);
        log.info("Storing objects...");
        final List<LockedEntity> lst = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            LockedEntity le = new LockedEntity();
            le.value = i;
            lst.add(le);
        }

        morphium.storeList(lst);
        log.info("done");
        TestUtils.waitForConditionToBecomeTrue(5000, "Storing failed", ()->morphium.createQueryFor(LockedEntity.class).countAll() == 1000);
        int numThreads = 5;
        AtomicInteger errors = new AtomicInteger();
        Vector<MorphiumId> ids = new Vector<>();

        for (int i = 0; i < numThreads; i++) {
            String lockId = "Thr" + i;
            Runnable r = new Runnable() {
                public void run() {
                    var q = morphium.createQueryFor(LockedEntity.class).sort("value");

                    for (int j = 0; j < 10; j++) {
                        q.limit(5);

                        try {
                            var lockedEntities = morphium.lockEntities(q, lockId, 1000);
                            log.info("Locked " + lockId + ": " + lockedEntities.size());

                            for (var le : lockedEntities) {
                                if (ids.contains(le.id)) {
                                    log.error("Duplicate ID - Locking failed!");
                                    errors.incrementAndGet();
                                } else {
                                    ids.add(le.id);
                                }

                                morphium.delete(le); //need to make sure, the entity is not found by query!
                            }
                        } catch (MorphiumDriverException e) {
                            e.printStackTrace();
                        }
                    }

                    q.limit(1);

                    try {
                        var lockedEntity = morphium.lockEntities(q, lockId, 1000).get(0);
                        log.info("releasing lock to " + lockedEntity.id.toString());
                        morphium.releaseLock(lockedEntity);
                    } catch (MorphiumDriverException e) {
                        e.printStackTrace();
                    }
                }
            };
            new Thread(r).start();
        }

        Thread.sleep(5000);
        assertEquals(0, errors.get(), "Errors during processing");
    }

    @Lockable @Entity
    public static class LockedEntity {
        @Id
        MorphiumId id;
        public int value;
        @LockedBy
        public String lockedBy;
        @LockedAt
        public long lockedAt;

    }
}
