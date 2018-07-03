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
        morphium.startTransaction();
        UncachedObject u = new UncachedObject("test", 101);
        morphium.store(u);
        assert (morphium.createQueryFor(UncachedObject.class).countAll() == 11);
        morphium.abortTransaction();
        Thread.sleep(1000);
        assert (morphium.createQueryFor(UncachedObject.class).countAll() == 10);

    }
}
