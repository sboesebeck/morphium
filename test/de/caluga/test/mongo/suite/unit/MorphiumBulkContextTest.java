package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by stephan on 18.11.15.
 */
public class MorphiumBulkContextTest extends MongoTest {
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
        UncachedObject uc = morphium.createQueryFor(UncachedObject.class).f("counter").eq(40).get();
        ArrayList<UncachedObject> lst = new ArrayList<>();
        lst.add(uc);
        c.addDeleteRequest(lst);
        assert (c.getNumberOfRequests() == 1);

    }

    @Test
    public void testAddDeleteRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addDeleteRequest(morphium.createQueryFor(UncachedObject.class).f("counter").eq(30).get());
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
        c.addCustomUpdateRequest(morphium.createQueryFor(UncachedObject.class).f("counter").eq(20), morphium.getMap("$set", morphium.getMap("counter", -20)), false, false);
        assert (c.getNumberOfRequests() == 1);
        
    }

    @Test
    public void testAddSetRequest() throws Exception {

    }

    @Test
    public void testAddUnSetRequest() throws Exception {

    }

    @Test
    public void testAddSetRequest1() throws Exception {

    }

    @Test
    public void testAddUnsetRequest() throws Exception {

    }

    @Test
    public void testAddIncRequest() throws Exception {

    }

    @Test
    public void testAddIncRequest1() throws Exception {

    }

    @Test
    public void testAddCurrentDateRequest() throws Exception {

    }

    @Test
    public void testAddCurrentDateRequest1() throws Exception {

    }

    @Test
    public void testAddMinRequest() throws Exception {

    }

    @Test
    public void testAddMinRequest1() throws Exception {

    }

    @Test
    public void testAddMinRequest2() throws Exception {

    }

    @Test
    public void testAddMaxRequest() throws Exception {

    }

    @Test
    public void testAddMaxRequest1() throws Exception {

    }

    @Test
    public void testAddMaxRequest2() throws Exception {

    }

    @Test
    public void testAddRenameRequest() throws Exception {

    }

    @Test
    public void testAddRenameRequest1() throws Exception {

    }

    @Test
    public void testAddMulRequest() throws Exception {

    }

    @Test
    public void testAddMulRequest1() throws Exception {

    }

    @Test
    public void testAddPopRequest() throws Exception {

    }

    @Test
    public void testAddPopRequest1() throws Exception {

    }

    @Test
    public void testAddPushRequest() throws Exception {

    }

    @Test
    public void testAddPushRequest1() throws Exception {

    }

    @Test
    public void testAddSetRequest2() throws Exception {

    }

    @Test
    public void testAddUnsetRequest1() throws Exception {

    }

    @Test
    public void testAddIncRequest2() throws Exception {

    }

    @Test
    public void testAddPushRequest2() throws Exception {

    }
}