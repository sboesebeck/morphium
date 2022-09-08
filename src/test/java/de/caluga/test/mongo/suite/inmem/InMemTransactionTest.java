package de.caluga.test.mongo.suite.inmem;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:01
 * <p>
 * TODO: Add documentation here
 */
public class InMemTransactionTest extends MorphiumInMemTestBase {

    @Test
    public void transactionTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(10);
        morphium.startTransaction();
        UncachedObject u = new UncachedObject("test", 101);
        morphium.store(u);
        long l = morphium.createQueryFor(UncachedObject.class).countAll();
        assert (l == 11) : "Count wrong: " + l;
        morphium.abortTransaction();
        Thread.sleep(1000);
        assert (morphium.createQueryFor(UncachedObject.class).countAll() == 10);

    }

}
