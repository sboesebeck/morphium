package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.AsyncWrites;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PreStore;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.13
 * Time: 13:05
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings("AssertWithSideEffects")
public class AsyncOperationTest extends MultiDriverTestBase {
    private boolean asyncCall = false;
    private boolean callback;

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void asyncStoreTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("=======> Running with "+morphium.getDriver().getName());

            asyncCall = false;
            super.createCachedObjects(morphium, 1000);
            TestUtils.waitForWrites(morphium,log);
            log.info("Uncached object preparation");
            super.createUncachedObjects(morphium, 1000);
            TestUtils.waitForWrites(morphium,log);
            Query<UncachedObject> uc = morphium.createQueryFor(UncachedObject.class);
            uc = uc.f(UncachedObject.Fields.counter).lt(100);
            log.info("deleting...");
            morphium.delete(uc, new AsyncCallbackAdapter<Query<UncachedObject>>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<Query<UncachedObject>> q, long duration, List<Query<UncachedObject>> result, Query<UncachedObject> entity, Object... param) {
                    log.info("Objects deleted");
                    asyncCall = true;
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query<Query<UncachedObject>> q, long duration, String error, Throwable t, Query<UncachedObject> entity, Object... param) {
                    assert false;
                }
            });
            Thread.sleep(200);
            assert(asyncCall);
            asyncCall = false;
            uc = uc.q();
            uc.f(UncachedObject.Fields.counter).mod(3, 2);
            log.info("Updating...");
            morphium.set(uc, UncachedObject.Fields.counter, 0, false, true, new AsyncOperationCallback<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    log.info("Objects updated");
                    asyncCall = true;
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                    log.info("Objects update error", t);
                }
            });
            TestUtils.waitForWrites(morphium,log);
            Thread.sleep(100);
            long counter = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(0).countAll();
            assert counter > 0 : "Counter is: " + counter;
            assert(asyncCall);
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void asyncReadTest(Morphium morphium) throws Exception {
        try (morphium) {
            asyncCall = false;
            createUncachedObjects(morphium, 100);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f(UncachedObject.Fields.counter).lt(1000);
            q.asList(new AsyncCallbackAdapter<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    asyncCall = true;
                    log.info("got read answer");
                    assertNotNull(result, "Error");
                    assert(result.size() == 100) : "Error";
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                    assert false;
                }
            });
            waitForAsyncOperationsToStart(morphium, 3000);
            int count = 0;

            while (q.getNumberOfPendingRequests() > 0) {
                count++;
                assert(count < 10);
                System.out.println("Still waiting...");
                Thread.sleep(1000);
            }

            assert(asyncCall);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void asyncCountTest(Morphium morphium) throws Exception {
        try (morphium) {
            asyncCall = false;
            createUncachedObjects(morphium, 100);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f(UncachedObject.Fields.counter).lt(1000);
            q.countAll(new AsyncOperationCallback<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    asyncCall = true;
                    log.info("got async callback!");
                    assertTrue(param != null && param[0] != null);
                    ;
                    assert(param[0].equals((long) 100));
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                    //To change body of implemented methods use File | Settings | File Templates.
                    log.error("got async error callback", t);
                    //noinspection ConstantConditions
                    assert(false);
                }
            });
            //waiting for thread to become active
            waitForAsyncOperationsToStart(morphium, 3000);
            Thread.sleep(2000);
            int count = 0;

            while (q.getNumberOfPendingRequests() > 0) {
                count++;
                assert(count < 10);
                Thread.sleep(1000);
            }

            assert(asyncCall);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testAsyncWriter(Morphium morphium) throws Exception {
        log.info("Running Test with "+morphium.getDriver().getName());
        try (morphium) {
            morphium.getDriver().setMaxWaitTime(15000);
            // morphium.dropCollection(AsyncObject.class);
            morphium.ensureIndicesFor(AsyncObject.class);
            Thread.sleep(2000);
            //assert (morphium.getDriver().exists("morphium_test", "async_object"));
            var q = morphium.createQueryFor(AsyncObject.class);
            assertEquals(q.countAll(), 0);
            Runnable r=()->{
                for (int i = 0; i < 125; i++) {
                    if (i % 10 == 0) {
                        log.info("Stored " + i + " objects");
                    }

                    AsyncObject ao = new AsyncObject();
                    ao.setCounter(i);
                    ao.setStrValue("Async write");
                    morphium.store(ao);
                    // Thread.sleep(5); //TODO: Get RID of this. this fixes the test, but the unerlying problem still exists
                    //                log.info("--------> Written: " + q.countAll());
                    //                log.info("Connections in pool : " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_POOL));
                    //                log.info("Connections borrowed: " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED));
                    //                log.info("Connections returned: " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_RELEASED));
                }
            };
            new Thread(r).start();
            new Thread(r).start();
            new Thread(r).start();
            new Thread(r).start();
            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < 5000 && q.countAll() != 500) {
                log.info("--------> Waiting: " + q.countAll());
                log.info("Connections in pool : " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_POOL));
                log.info("Connections borrowed: " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED));
                log.info("Connections returned: " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_RELEASED));
                Thread.sleep(500);
            }

            log.info("--------> finished: " + q.countAll());
            log.info("Connections in pool : " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_POOL));
            log.info("Connections borrowed: " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED));
            log.info("Connections returned: " + morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_RELEASED));
            assertTrue(q.countAll() != 0);
            assertEquals(500, q.countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void asyncErrorHandling(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("upcoming Error Message + Exception is expected");
            WrongObject wo = new WrongObject();
            callback = false;
            morphium.store(wo, new AsyncOperationCallback<WrongObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<WrongObject> q, long duration, List<WrongObject> result, WrongObject entity, Object... param) {
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query<WrongObject> q, long duration, String error, Throwable t, WrongObject entity, Object... param) {
                    log.info("On Error Callback called correctly");
                    callback = true;
                }
            });
            Thread.sleep(1000);
            assertTrue(callback,"Callback not called");
        }
    }

    @AsyncWrites
    @Entity
    @WriteSafety(level = SafetyLevel.NORMAL)
    @Lifecycle
    public static class WrongObject {
        @Id
        private MorphiumId id;
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @PreStore
        public void preStore() {
            throw new RuntimeException("error");
        }
    }

    @AsyncWrites
    public static class AsyncObject extends UncachedObject {

    }
}
