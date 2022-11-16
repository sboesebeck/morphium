package de.caluga.test.mongo.suite.locking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.locking.Lockable;
import de.caluga.morphium.annotations.locking.LockedAt;
import de.caluga.morphium.annotations.locking.LockedBy;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class LockEntityTest extends MorphiumTestBase {


    @Test
    public void simpleLocking() throws Exception {
        morphium.dropCollection(LockedEntity.class);
        for (int i=0;i<100;i++){
            LockedEntity le=new LockedEntity();
            le.value=i;
            morphium.store(le);
        }
        //locking

        var q=morphium.createQueryFor(LockedEntity.class).f("value").gt(42).limit(2);
        var res=morphium.lockEntities(q, "Its me", 1000);
        assertEquals(2, res.size(), "Not all locked! "+res.size());
        var l=q.asList();
        log.info("Done");
    }




    @Lockable @Entity
    public static class LockedEntity{
        @Id
        MorphiumId id;
            public int value;
        @LockedBy
        public String lockedBy;
        @LockedAt
        public long lockedAt;

    }
}
