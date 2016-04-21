package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.AsyncWrites;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.13
 * Time: 13:05
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings("AssertWithSideEffects")
public class AsyncOperationTest extends MongoTest {
    private boolean asyncCall = false;
    private boolean callback;

    @Test
    public void asyncStoreTest() throws Exception {
        asyncCall = false;
        super.createCachedObjects(1000);
        waitForWrites();
        log.info("Uncached object preparation");
        super.createUncachedObjects(1000);
        waitForWrites();
        Query<UncachedObject> uc = morphium.createQueryFor(UncachedObject.class);
        uc = uc.f("counter").lt(100);
        log.info("deleting...");
        morphium.delete(uc, new AsyncOperationCallback<Query<UncachedObject>>() {
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
        Thread.sleep(100);
        assert (asyncCall);
        asyncCall = false;
        uc = uc.q();
        uc.f("counter").mod(3, 2);
        log.info("Updating...");
        morphium.set(uc, "counter", 0, false, true, new AsyncOperationCallback<UncachedObject>() {
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

        waitForWrites();
        Thread.sleep(100);

        long counter = morphium.createQueryFor(UncachedObject.class).f("counter").eq(0).countAll();
        assert counter > 0 : "Counter is: " + counter;
        assert (asyncCall);
    }


    @Test
    public void asyncReadTest() throws Exception {
        asyncCall = false;
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(1000);
        q.asList(new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                asyncCall = true;
                log.info("got read answer");
                assert (result != null) : "Error";
                assert (result.size() == 100) : "Error";
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                assert false;
            }
        });
        waitForAsyncOperationToStart(1000000);
        int count = 0;
        while (q.getNumberOfPendingRequests() > 0) {
            count++;
            assert (count < 10);
            System.out.println("Still waiting...");
            Thread.sleep(1000);
        }
        assert (asyncCall);
    }

    @Test
    public void asyncCountTest() throws Exception {
        asyncCall = false;
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(1000);
        q.countAll(new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                asyncCall = true;
                log.info("got async callback!");
                assert (param != null && param[0] != null);
                assert (param[0].equals((long) 100));
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                //To change body of implemented methods use File | Settings | File Templates.
                log.error("got async error callback", t);
                assert (false);
            }
        });
        //waiting for thread to become active
        waitForAsyncOperationToStart(1000000);
        Thread.sleep(2000);
        int count = 0;
        while (q.getNumberOfPendingRequests() > 0) {
            count++;
            assert (count < 10);
            Thread.sleep(1000);
        }
        assert (asyncCall);
    }

    @Test
    public void testAsyncWriter() throws Exception {
        morphium.dropCollection(AsyncObject.class);
        morphium.ensureIndicesFor(AsyncObject.class);
        Thread.sleep(1000);
        assert (morphium.getDriver().exists("morphium_test", "async_object"));

        long start = System.currentTimeMillis();
        for (int i = 0; i < 500; i++) {
            if (i % 10 == 0) {
                log.info("Stored " + i + " objects");
            }
            AsyncObject ao = new AsyncObject();
            ao.setCounter(i);
            ao.setValue("Async write");
            morphium.store(ao);
        }

        long end = System.currentTimeMillis();
        assert (end - start < 8000);
        waitForWrites();
        Thread.sleep(100);
        assert (morphium.createQueryFor(AsyncObject.class).countAll() != 0);
        assert (morphium.createQueryFor(AsyncObject.class).countAll() == 500);
    }

    @Test
    public void asyncErrorHandling() throws Exception {
        log.info("upcoming Error Message + Exception is expected");
        WrongObject wo = new WrongObject();
        morphium.store(wo);

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
        assert (callback);

    }


    @AsyncWrites
    @Entity
    @WriteSafety(level = SafetyLevel.NORMAL)
    public static class WrongObject {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @AsyncWrites
    public static class AsyncObject extends UncachedObject {

    }
}
