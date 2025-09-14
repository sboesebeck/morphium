package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.*;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 29.04.14
 * Time: 08:12
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings({"AssertWithSideEffects", "unchecked"})
@Tag("core")
public class BulkOperationTest extends MultiDriverTestBase {
    private boolean preRemove, postRemove;
    private boolean preUpdate, postUpdate;


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void bulkTest2(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);

            createUncachedObjects(morphium, 10);
            TestUtils.waitForWrites(morphium, log);

            UncachedObject uc1 = morphium.createQueryFor(UncachedObject.class).get();
            long s = System.currentTimeMillis();
            while (uc1 == null) {
                uc1 = morphium.createQueryFor(UncachedObject.class).get();
                Thread.sleep(100);
                assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            }

            MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
            //        UpdateBulkRequest up = c
            c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 999, true, true);
            c.addIncRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 10, false, false);
            c.addInsertRequest(Arrays.asList(new UncachedObject("test123", 123)));
            c.addCurrentDateRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), false, false, "date");
            c.addDeleteRequest(morphium.createQueryFor(UncachedObject.class).f("counter").lte(10), false);
            c.addIncRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), UtilsMap.of("counter", 12), false, false);
            c.addMulRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 2, false, false);
            c.addUnsetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), UtilsMap.of("date", 1), false, false);
            c.addUnSetRequest(uc1, "strValue", null, false);
            c.addSetRequest(uc1, "counter", 33, false);
            c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), UtilsMap.of("strValue", "f"), false, false);
            c.addCurrentDateRequest(uc1, "date", false);
            c.addMulRequest(uc1, "counter", 1, false);
            c.addDeleteRequest(uc1);
            c.addDeleteRequest(Arrays.asList(uc1));
            c.addMaxRequest(uc1, "counter", 1, false);
            c.addMaxRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 1, false, false);
            c.addMaxRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), UtilsMap.of("counter", 12), false, false);
            c.addMinRequest(uc1, "counter", 1, false);
            c.addPopRequest(uc1, "lst", false);
            c.addPushRequest(uc1, "lst", "test", false);
            c.addPushRequest(uc1, "lst", Arrays.asList("test"), false);
            c.addPushRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "lst", Arrays.asList("test"), false, false);
            c.addIncRequest(uc1, "counter", 12, false);
            c.addRenameRequest(uc1, "date", "dt", false);
            Map<String, Object> ret = c.runBulk();
            Thread.sleep(500);

            for (UncachedObject o : morphium.createQueryFor(UncachedObject.class).asList()) {
                log.info(o.toString());
            }
        }

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void bulkTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            TestUtils.waitForWrites(morphium, log);

            MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
            //        UpdateBulkRequest up = c
            c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 999, true, true);
            Map<String, Object> ret = c.runBulk();
            long s = System.currentTimeMillis();
            while (morphium.createQueryFor(UncachedObject.class).f("counter").eq(999).countAll() != 100) {
                Thread.sleep(100);
                assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            }
            Thread.sleep(1000);
            for (UncachedObject o : morphium.createQueryFor(UncachedObject.class).asList()) {
                assert (o.getCounter() == 999) : "Counter is " + o.getCounter();
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            createUncachedObjects(morphium, 100);

            MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
            c.addIncRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 1000, true, true);
            c.runBulk();
            long s = System.currentTimeMillis();
            while (morphium.createQueryFor(UncachedObject.class).f("counter").gte(1000).countAll() != 100) {
                Thread.sleep(100);
                assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            }
            Thread.sleep(1000);
            for (UncachedObject o : morphium.createQueryFor(UncachedObject.class).asList()) {
                if (o.getCounter() <= 1000) {
                    log.error("Counter is < 1000!?");
                    morphium.reread(o);
                }
                assert (o.getCounter() > 1000) : "Counter is " + o.getCounter() + " - Total number: " + TestUtils.countUC(morphium) + " greater than 1000: " + morphium.createQueryFor(UncachedObject.class).f("counter").gte(1000).countAll();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void callbackTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);

            MorphiumStorageListener<UncachedObject> listener = new MorphiumStorageAdapter<UncachedObject>() {
                @Override
                public void preRemove(Morphium m, Query<UncachedObject> q) {
                    preRemove = true;
                }

                @Override
                public void postRemove(Morphium m, Query<UncachedObject> q) {
                    postRemove = true;
                }

                @Override
                public void preUpdate(Morphium m, Class <? extends UncachedObject > cls, Enum updateType) {
                    preUpdate = true;
                }

                @Override
                public void postUpdate(Morphium m, Class <? extends UncachedObject > cls, Enum updateType) {
                    postUpdate = true;
                }
            };

            morphium.addListener(listener);
            preUpdate = postUpdate = preRemove = postRemove = false;
            incTest(morphium);
            Thread.sleep(1500);
            assert (preUpdate);
            assert (postUpdate);
            assert (!preRemove);
            assert (!postRemove);
            morphium.removeListener(listener);
        }
    }

}
