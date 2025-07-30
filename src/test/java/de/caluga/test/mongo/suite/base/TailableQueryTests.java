package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CappedCol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by stephan on 29.07.16.
 */

public class TailableQueryTests extends MultiDriverTestBase {
    AtomicInteger found = new AtomicInteger(0);

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void tailableTest(Morphium m) throws Exception {
        var orig = m.getConfig().getCappedCheck();
        m.getConfig().setCappedCheck(CappedCheck.CREATE_ON_WRITE_NEW_COL);

        try (m) {
            Morphium m2 = m;

            if (m.getDriver().getName().equals(SingleMongoConnectDriver.driverName)) {
                var cfg = MorphiumConfig.fromProperties(m.getConfig().asProperties());
                cfg.setCredentialsDecryptionKey(m.getConfig().getCredentialsDecryptionKey());
                cfg.setCredentialsEncryptionKey(m.getConfig().getCredentialsEncryptionKey());
                m2 = new Morphium(cfg);
            }

            m.dropCollection(CappedCol.class);
            TestUtils.waitForConditionToBecomeTrue(1000, "Collection not dropped??", ()->!m.exists(m.getDatabase(), m.getMapper().getCollectionName(CappedCol.class)));
            CappedCol o = new CappedCol("Test 1", 1);
            m.store(o);
            m.store(new CappedCol("Test 2", 2));
            Thread.sleep(100);
            new Thread(()-> {
                Query<CappedCol> q = m.createQueryFor(CappedCol.class);
                q.tail(10, 0, new AsyncCallbackAdapter<>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<CappedCol> q, long duration, List<CappedCol> result, CappedCol entity, Object ... param) {
                        log.info("Got incoming!!! " + entity.getStrValue() + " " + entity.getCounter());
                        found.incrementAndGet();

                        if (entity.getStrValue().equals("Test 3 - quit")) {
                            throw new MorphiumAccessVetoException("Quitting");
                        }
                    }
                });
                assertTrue(found.get() >= 1);
            }).start();
            TestUtils.waitForConditionToBecomeTrue(2500, "no result coming in?", ()->found.get() == 2);
            log.info("Storing 3...");
            m2.store(new CappedCol("Test 3 - quit", 3));
            log.info("Stored... waiting for event");
            TestUtils.waitForConditionToBecomeTrue(2500, "3rd result not coming in", ()->found.get() == 3);

            if (m.getDriver().getName().equals(SingleMongoConnectDriver.driverName)) {
                m2.close();
            }

            m.getConfig().setCappedCheck(orig);
        }
    }

}
