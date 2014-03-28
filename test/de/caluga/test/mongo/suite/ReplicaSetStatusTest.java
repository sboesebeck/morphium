package de.caluga.test.mongo.suite;

import com.mongodb.WriteConcern;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 24.08.12
 * Time: 11:39
 * <p/>
 */
public class ReplicaSetStatusTest extends MongoTest {
    private static Logger log = Logger.getLogger(ReplicaSetStatusTest.class);

    @Test
    public void testReplicaSetMonitoring() throws Exception {
        int cnt = 0;
        while (MorphiumSingleton.get().getCurrentRSState() == null) {
            cnt++;
            assert (cnt < 7);
            Thread.sleep(1000);
        }

        log.info("got status: " + MorphiumSingleton.get().getCurrentRSState().getActiveNodes());
    }

    @Test
    public void testWriteConcern() throws Exception {
        WriteConcern w = MorphiumSingleton.get().getWriteConcernForClass(SecureObject.class);
        assert (w.getW() == 2);
        assert (w.getJ());
        assert (!w.getFsync());
        assert (w.getWtimeout() == 10000);
        assert (w.raiseNetworkErrors());
    }

    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES, waitForSync = true, waitForJournalCommit = true, timeout = 10000)
    public class SecureObject extends UncachedObject {

    }
}
