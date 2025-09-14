package de.caluga.test.mongo.suite.inmem;

import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.UncachedObject;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:01
 * <p>
 */
public class InMemTransactionTest extends MorphiumInMemTestBase {

    @Test
    @Tag("core")
    public void transactionTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(10);
        morphium.startTransaction();
        UncachedObject u = new UncachedObject("test", 101);
        morphium.store(u);
        long l = TestUtils.countUC(morphium);
        assert (l == 11) : "Count wrong: " + l;
        morphium.abortTransaction();
        Thread.sleep(1000);
        assertEquals(10, TestUtils.countUC(morphium));

    }

}
