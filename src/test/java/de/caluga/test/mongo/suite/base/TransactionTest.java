package de.caluga.test.mongo.suite.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:01
 * <p>
 */
@Tag("core")
public class TransactionTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void transactionTest(Morphium morphium) throws Exception  {
        // Skip for MorphiumServer - transactions require connection affinity which PooledDriver
        // doesn't maintain when connecting to MorphiumServer. Each operation in a transaction
        // may use a different connection, causing transaction context to be lost.
        if (morphium.getDriver().isInMemoryBackend() && !morphium.getDriver().getName().equals("InMemDriver")) {
            log.info("Skipping transaction test for MorphiumServer - connection affinity not maintained");
            morphium.close();
            return;
        }
        for (int i = 0; i < 10; i++) {
            try {
                morphium.createQueryFor(UncachedObject.class).delete();
                TestUtils.waitForConditionToBecomeTrue(5000, "did not clear", ()->TestUtils.countUC(morphium) == 0);
                createUncachedObjects(morphium, 10);
                morphium.startTransaction();
                log.info("Transaction started: " + morphium.getTransaction().getTxnNumber());
                log.info("Count after transaction start: " + TestUtils.countUC(morphium));
                UncachedObject u = new UncachedObject("test", 101);
                morphium.store(u);
                TestUtils.waitForConditionToBecomeTrue(5000, "Did not write during transaction", ()->TestUtils.countUC(morphium) == 11);
                long cnt = TestUtils.countUC(morphium);
                log.info("Count during transaction " + cnt);
                morphium.inc(u, "counter", 1);
                log.info("inc on element");
                Thread.sleep(100);
                u = morphium.reread(u);
                assertEquals(102, u.getCounter());

                morphium.createQueryFor(UncachedObject.class).f("counter").eq(2).set("counter", 111);
                TestUtils.waitForConditionToBecomeTrue(500, "counter not updated", ()->morphium.createQueryFor(UncachedObject.class).f("counter").eq(111).countAll() == 1);
                log.info("Aborting Transaction");
                morphium.abortTransaction();
                Thread.sleep(100);
                cnt = TestUtils.countUC(morphium);
                log.info("count after rollback: " + cnt);

                u = morphium.reread(u);
                assertNull(u);

                assertEquals(10, cnt, "Count after rollback: " + cnt);
            } catch (Exception e) {
                log.error("ERROR", e);
                if (morphium.getTransaction() != null) {
                    morphium.abortTransaction();
                }
            }
        }
    }


}
