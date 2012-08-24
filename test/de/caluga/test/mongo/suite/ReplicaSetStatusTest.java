package de.caluga.test.mongo.suite;

import com.mongodb.WriteConcern;
import de.caluga.morphium.MongoDbMode;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.replicaset.ReplicaSetStatus;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 24.08.12
 * Time: 11:39
 * <p/>
 * TODO: Add documentation here
 */
public class ReplicaSetStatusTest extends MongoTest {
    private static Logger log = Logger.getLogger(ReplicaSetStatusTest.class);

    @Test
    public void testReplicaSetStatus() throws Exception {
        if (!MorphiumSingleton.get().getConfig().getMode().equals(MongoDbMode.REPLICASET)) {
            log.warn("Not testing replicaset-status - not configured as such!");
            return;
        }

        ReplicaSetStatus stat = MorphiumSingleton.get().getReplicaSetStatus(true);
        log.info("Stat \n" + stat.toString());
        assert (stat.getActiveNodes() == 3);
    }

    @Test
    public void testWriteConcer() throws Exception {
        WriteConcern w = MorphiumSingleton.get().getWriteConcernForClass(UncachedObject.class);
        assert (w.getW() == 3) : "W is wrong: " + w.getW();

    }
}
