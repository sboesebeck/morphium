package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by stephan on 18.11.15.
 */
public class BulkContextTest extends MongoTest {
    boolean cb = false;

    @Test
    public void testRunBulk() throws Exception {
        createUncachedObjects(100);
        //        MorphiumBulkContext<UncachedObject> c=testAddInsertRequest();
        //        c.runBulk();
        //        c=testAddDeleteRequest();
        //        c.runBulk();
        //        c=testAddDeleteRequest1();
        //        c.runBulk();

    }

    @Test
    public void testRunBulk1() throws Exception {
        createUncachedObjects(100);
        AsyncOperationCallback callback = new AsyncOperationCallback() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query q, long duration, List result, Object entity, Object... param) {
                log.info("Got callback!");
                cb = true;
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query q, long duration, String error, Throwable t, Object entity, Object... param) {
                log.error("Got error: ", t);
            }
        };
        //        MorphiumBulkContext<UncachedObject> c=testAddInsertRequest();
        //        c.runBulk(callback);
        //        cb=false;
        //        int count=100;
        //        while(cb==false) {
        //            Thread.sleep(100);
        //            count--;
        //            assert(count>0);
        //        }
        //        c=testAddDeleteRequest();
        //        c.runBulk(callback);
        //        count=100;
        //        while(cb==false) {
        //            Thread.sleep(100);
        //            count--;
        //            assert(count>0);
        //        }
        //
        //        c=testAddDeleteRequest1();
        //        c.runBulk(callback);
        //        count=100;
        //        while(cb==false) {
        //            Thread.sleep(100);
        //            count--;
        //            assert(count>0);
        //        }
    }

    @Test
    public void testAddInsertRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addInsertRequest(new ArrayList<>());
        assert (c.getNumberOfRequests() == 1);

    }

    @Test
    public void testAddDeleteRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addDeleteRequest(new ArrayList<>());
        assert (c.getNumberOfRequests() == 0);
        UncachedObject uc = new UncachedObject();//morphium.createQueryFor(UncachedObject.class).f("counter").eq(40).get();
        uc.setMorphiumId(new MorphiumId());
        ArrayList<UncachedObject> lst = new ArrayList<>();
        lst.add(uc);
        c.addDeleteRequest(lst);
        assert (c.getNumberOfRequests() == 1);

    }

    @Test
    public void testAddDeleteRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        UncachedObject uc = new UncachedObject();//morphium.createQueryFor(UncachedObject.class).f("counter").eq(40).get();
        uc.setMorphiumId(new MorphiumId());
        c.addDeleteRequest(uc);
        assert (c.getNumberOfRequests() == 1);

    }

    @Test
    public void testAddDeleteRequest2() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addDeleteRequest(morphium.createQueryFor(UncachedObject.class), false);
        assert (c.getNumberOfRequests() == 1);

    }

    @Test
    public void testAddCustomUpdateRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addCustomUpdateRequest(morphium.createQueryFor(UncachedObject.class).f("counter").eq(20), Utils.getMap("$set", Utils.getMap("counter", -20)), false, false);
        assert (c.getNumberOfRequests() == 1);

    }

    @Test
    public void testAddSetRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addSetRequest(morphium.createQueryFor(UncachedObject.class), "counter", 1, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddUnSetRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addUnsetRequest(morphium.createQueryFor(UncachedObject.class), "counter", 1, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddSetRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addSetRequest(morphium.createQueryFor(UncachedObject.class), Utils.getMap("counter", 1), false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddUnsetRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addUnsetRequest(morphium.createQueryFor(UncachedObject.class), "counter", 1, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddIncRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addIncRequest(morphium.createQueryFor(UncachedObject.class), "counter", 1, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddIncRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addSetRequest(morphium.createQueryFor(UncachedObject.class), Utils.getMap("counter", 1), false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddCurrentDateRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addCurrentDateRequest(morphium.createQueryFor(UncachedObject.class), false, false, "counter");
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddCurrentDateRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addCurrentDateRequest(new UncachedObject(), "counter", false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMinRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMinRequest(morphium.createQueryFor(UncachedObject.class), "counter", 123, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMinRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMinRequest(morphium.createQueryFor(UncachedObject.class), Utils.getMap("counter", 123), false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMinRequest2() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMinRequest(new UncachedObject(), "counter", 123, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMaxRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMaxRequest(morphium.createQueryFor(UncachedObject.class), "counter", 123, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMaxRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMaxRequest(morphium.createQueryFor(UncachedObject.class), Utils.getMap("counter", 123), false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMaxRequest2() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMaxRequest(new UncachedObject(), "counter", 123, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddRenameRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addRenameRequest(morphium.createQueryFor(UncachedObject.class), "counter", "new_counter", false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddRenameRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addRenameRequest(new UncachedObject(), "counter", "new_counter", false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMulRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMulRequest(morphium.createQueryFor(UncachedObject.class), "counter", 123, false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddMulRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addMulRequest(new UncachedObject(), "counter", 123, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddPopRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addPopRequest(new UncachedObject(), "counter", false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddPopRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addPopRequest(morphium.createQueryFor(UncachedObject.class), "counter", false, false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddPushRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addPushRequest(new UncachedObject(), "counter", "value", false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddPushRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addPushRequest(morphium.createQueryFor(UncachedObject.class), "counter", "value", false, false);
        assert (c.getNumberOfRequests() == 1);
    }


}