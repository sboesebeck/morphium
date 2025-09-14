package de.caluga.test.mongo.suite.base;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.UncachedObject;

@Tag("core")
public class MultiUpdateTests extends MorphiumTestBase {

    @Test
    public void mutlipleUpdateTests() throws Exception {
        createUncachedObjects(1000);
        TestUtils.waitForConditionToBecomeTrue(1000, "not stored?!?!?", ()->morphium.createQueryFor(UncachedObject.class).countAll() == 1000);
        Runnable lock = ()->{
            String lockId = UUID.randomUUID().toString();
            var q = morphium.createQueryFor(UncachedObject.class).f("strValue").ne(lockId).sort("counter");
            var start = System.currentTimeMillis();
            var r = q.set("strValue", lockId, true, true);
            log.info(" Update result   : " + Utils.toJsonString(r));
            var locked = q.q().f("strValue").eq(lockId).asList();
            int lockedSize = 0;
            for (var uc : locked) {
                uc = morphium.reread(uc);
                if (uc.getStrValue().equals(lockId)) {
                    lockedSize++;
                }
            }
            log.info("Items locked     : " + locked.size());
            log.info("Items lockcheck  : " + lockedSize);
            var dur = System.currentTimeMillis() - start;
            log.info("Duration update  : " + dur);


        };
        var t1 = new Thread(lock);
        t1.setName("t1");
        var t2 = new Thread(lock);
        t2.setName("t2");
        var t3 = new Thread(lock);
        t3.setName("t3");
        var s1 = System.currentTimeMillis();
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        log.info("All threads took: " + (System.currentTimeMillis() - s1));
        lock = ()->{
            var start = System.currentTimeMillis();
            var lst = morphium.createQueryFor(UncachedObject.class).sort("counter").idList();
            var toStore = new ArrayList<UCLock>();

            for (Object obj : lst) {
                MorphiumId uc = (MorphiumId)obj;
                UCLock u = new UCLock();
                u.id = uc;
                toStore.add(u);
            }
            try {
                morphium.store(toStore);
            } catch (Exception e) {
            }
            var dur = System.currentTimeMillis() - start;
            log.info("Duration store: " + dur);

        };

        t1 = new Thread(lock);
        t1.setName("t1");
        t2 = new Thread(lock);
        t2.setName("t2");
        t3 = new Thread(lock);
        t3.setName("t3");
        var start = System.currentTimeMillis();
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        var dur = System.currentTimeMillis() - start;
        log.info("Joining threads: " + dur);
        log.info(" Locked: " + morphium.createQueryFor(UCLock.class).countAll());


    }

    @Entity
    public static class UCLock {
        @Id
        public MorphiumId id;

    }

}
