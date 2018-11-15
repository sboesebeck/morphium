package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.TestEntityNameProvider;
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
        for (int i = 0; i < 10; i++) {
            try {
                morphium.createQueryFor(UncachedObject.class).delete();
                Thread.sleep(500);
                TestEntityNameProvider.number.incrementAndGet();
                log.info("Entityname number: " + TestEntityNameProvider.number.get());
                createUncachedObjects(10);
                Thread.sleep(500);
                morphium.startTransaction();
                Thread.sleep(500);
                log.info("Count after transaction start: " + morphium.createQueryFor(UncachedObject.class).countAll());
                UncachedObject u = new UncachedObject("test", 101);
                morphium.store(u);
                Thread.sleep(500);
                long cnt = morphium.createQueryFor(UncachedObject.class).countAll();
                if (cnt != 11) {
                    morphium.abortTransaction();
                    assert (cnt == 11) : "Count during transaction: " + cnt;
                }
                morphium.abortTransaction();
                Thread.sleep(1000);
                cnt = morphium.createQueryFor(UncachedObject.class).countAll();
                assert (cnt == 10) : "Count after rollback: " + cnt;
            } catch (Exception e) {
                morphium.abortTransaction();
            }
        }

    }

}
