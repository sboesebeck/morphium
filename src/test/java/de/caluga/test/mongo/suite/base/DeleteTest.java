package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 03.09.12
 * Time: 08:18
 * <p>
 */
@Tag("core")
public class DeleteTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void uncachedDeleteSingle(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 10);
            TestUtils.waitForConditionToBecomeTrue(1000, "create failed", () -> TestUtils.countUC(morphium) == 10);
            UncachedObject u = morphium.createQueryFor(UncachedObject.class).get();
            morphium.delete(u);
            TestUtils.waitForConditionToBecomeTrue(1000, "delete failed", () -> TestUtils.countUC(morphium) == 9);
            List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).asList();
            for (UncachedObject uc : lst) {
                assert (!uc.getMorphiumId().equals(u.getMorphiumId()));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void uncachedDeleteQuery(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 10);
            TestUtils.waitForConditionToBecomeTrue(1000, "Count!=10 still", () -> TestUtils.countUC(morphium) == 10);
            UncachedObject u = morphium.createQueryFor(UncachedObject.class).get();
            morphium.delete(morphium.createQueryFor(UncachedObject.class).f("counter").eq(u.getCounter()));
            TestUtils.waitForConditionToBecomeTrue(1000, "delete failed", () -> TestUtils.countUC(morphium) == 9);
            List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).asList();
            for (UncachedObject uc : lst) {
                assert (!uc.getMorphiumId().equals(u.getMorphiumId()));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cachedDeleteSingle(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            createCachedObjects(morphium, 10);
            TestUtils.waitForWrites(morphium, log);
            long c = morphium.createQueryFor(CachedObject.class).countAll();
            assert (c == 10) : "Count is " + c;
            CachedObject u = morphium.createQueryFor(CachedObject.class).get();
            morphium.delete(u);
            TestUtils.waitForWrites(morphium, log);

            String k = "X-Entries for: resultCache|de.caluga.test.mongo.suite.data.CachedObject";

            while (morphium.getStatistics().get(k) != null && morphium.getStatistics().get(k).intValue() != 0) {
                log.info("Waiting for cache to be cleared");
                Thread.sleep(250);
            }

            c = morphium.createQueryFor(CachedObject.class).countAll();
            assert (c == 9);
            List<CachedObject> lst = morphium.createQueryFor(CachedObject.class).asList();
            for (CachedObject uc : lst) {
                assert (!uc.getId().equals(u.getId()));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cachedDeleteQuery(Morphium morphium) throws Exception {
        try (morphium) {
            createCachedObjects(morphium, 10);
            TestUtils.waitForWrites(morphium, log);
            long cnt = morphium.createQueryFor(CachedObject.class).countAll();
            assert (cnt == 10) : "Count is " + cnt;
            CachedObject co = morphium.createQueryFor(CachedObject.class).get();
            morphium.delete(morphium.createQueryFor(CachedObject.class).f("counter").eq(co.getCounter()));
            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(100);
            cnt = morphium.createQueryFor(CachedObject.class).countAll();
            assert (cnt == 9);
            List<CachedObject> lst = morphium.createQueryFor(CachedObject.class).asList();
            for (CachedObject c : lst) {
                assert (!c.getId().equals(co.getId()));
            }
        }
    }


}
