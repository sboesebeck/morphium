package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig.CappedCheck;
import de.caluga.test.mongo.suite.data.CappedCol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by stephan on 08.08.14.
 */
@SuppressWarnings("AssertWithSideEffects")
public class CappedCollectionTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testCreationOfCappedCollection(Morphium morphium) throws Exception {
        log.info("Running test with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(CappedCol.class);
            Thread.sleep(1000);
            morphium.ensureCapped(CappedCol.class);
            CappedCol cc = new CappedCol();
            cc.setStrValue("A value");
            cc.setCounter(-1);
            morphium.store(cc);
            assert(morphium.getDriver().isCapped(morphium.getConfig().getDatabase(), "capped_col"));

            //storing more than max entries
            for (int i = 0; i < 1000; i++) {
                cc = new CappedCol();
                cc.setStrValue("Value " + i);
                cc.setCounter(i);
                morphium.store(cc);
            }

            Thread.sleep(1000);
            assert(morphium.createQueryFor(CappedCol.class).countAll() <= 10);

            for (CappedCol cp : morphium.createQueryFor(CappedCol.class).sort("counter").asIterable(10)) {
                log.info("Capped: " + cp.getCounter() + " - " + cp.getStrValue());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testAutCappedCollOnWrite(Morphium morphium) throws Exception {
        try(morphium) {
            var orig = morphium.getConfig().getCappedCheck();
            morphium.getConfig().setCappedCheck(CappedCheck.CREATE_ON_WRITE_NEW_COL);
            morphium.dropCollection(CappedCol.class);
            Thread.sleep(1000);
            assertFalse(morphium.exists(morphium.getDatabase(), "capped_col"));
            CappedCol cc = new CappedCol();
            cc.setStrValue("Some value");
            cc.setCounter(42);
            morphium.store(cc);
            //should be written, non existent hence capped
            assertTrue(morphium.getDriver().isCapped(morphium.getDatabase(), "capped_col"));
            morphium.getConfig().setCappedCheck(orig);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testListCreationOfCappedCollection(Morphium morphium) throws Exception {
        log.info("Running test with " + morphium.getDriver().getName());

        try (morphium) {
            var orig = morphium.getConfig().getCappedCheck();
            morphium.getConfig().setCappedCheck(CappedCheck.CREATE_ON_WRITE_NEW_COL);
            morphium.dropCollection(CappedCol.class);
            Thread.sleep(1000);
            assertFalse(morphium.exists(morphium.getDatabase(), "capped_col"));
            List<CappedCol> lst = new ArrayList<>();

            //storing more than max entries
            for (int i = 0; i < 100; i++) {
                CappedCol cc = new CappedCol();
                cc.setStrValue("Value " + i);
                cc.setCounter(i);
                lst.add(cc);
            }

            morphium.storeList(lst);
            Thread.sleep(100);
            assert(morphium.getDriver().isCapped(morphium.getConfig().getDatabase(), "capped_col"));
            assertTrue(morphium.createQueryFor(CappedCol.class).countAll() <= 10);

            for (CappedCol cp : morphium.createQueryFor(CappedCol.class).sort("counter").asIterable(10)) {
                log.info("Capped: " + cp.getCounter() + " - " + cp.getStrValue());
            }

            morphium.getConfig().setCappedCheck(orig);
        }
    }

    // @ParameterizedTest
    // @MethodSource("getMorphiumInstances")
    // public void convertToCappedTest(Morphium morphium) throws Exception {
    //     log.info("Running test with " + morphium.getDriver().getName());
    //
    //     try (morphium) {
    //         morphium.dropCollection(UncachedObject.class);
    //         createUncachedObjects(morphium, 1000);
    //         morphium.convertToCapped(UncachedObject.class, 100, 10, null);
    //         // assert (morphium.getDriver().isCapped(morphium.getConfig().getDatabase(), "uncached_object"));
    //         assert(morphium.createQueryFor(UncachedObject.class).countAll() <= 100);
    //     }
    // }

}
