package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:01
 * <p>
 * TODO: Add documentation here
 */
public class TransactionTest extends MongoTest {

    @Test
    public void transactionTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(10);
        Thread.sleep(500);
        morphium.startTransaction();
        log.info("Count now: " + morphium.createQueryFor(UncachedObject.class).countAll());
        UncachedObject u = new UncachedObject("test", 101);
        morphium.store(u);
        Thread.sleep(1500);
        long cnt = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (cnt == 11) : "Count wrong: " + cnt;
        morphium.abortTransaction();
        Thread.sleep(1000);
        cnt = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (cnt == 10) : "Count wrong: " + cnt;

    }

}
