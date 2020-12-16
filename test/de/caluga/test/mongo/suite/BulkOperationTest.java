package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.Utils;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

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
public class BulkOperationTest extends MorphiumTestBase {
    private boolean preRemove, postRemove;
    private boolean preUpdate, postUpdate;


    @Test
    public void bulkTest2() throws Exception {
        morphium.dropCollection(UncachedObject.class);

        createUncachedObjects(10);
        UncachedObject uc1 = morphium.createQueryFor(UncachedObject.class).get();
        waitForWrites();

        MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
        //        UpdateBulkRequest up = c
        c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 999, true, true);
        c.addIncRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 10, false, false);
        c.addInsertRequest(Arrays.asList(new UncachedObject("test123", 123)));
        c.addCurrentDateRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), false, false, "date");
        c.addDeleteRequest(morphium.createQueryFor(UncachedObject.class).f("counter").lte(10), false);
        c.addIncRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), Utils.getMap("counter", 12), false, false);
        c.addMulRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 2, false, false);
        c.addUnsetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), Utils.getMap("date", 1), false, false);
        c.addUnSetRequest(uc1, "value", null, false);
        c.addSetRequest(uc1, "counter", 33, false);
        c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), Utils.getMap("value", "f"), false, false);
        c.addCurrentDateRequest(uc1, "date", false);
        c.addMulRequest(uc1, "counter", 1, false);
        c.addDeleteRequest(uc1);
        c.addDeleteRequest(Arrays.asList(uc1));
        c.addMaxRequest(uc1, "counter", 1, false);
        c.addMaxRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 1, false, false);
        c.addMaxRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), Utils.getMap("counter", 12), false, false);
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

    @Test
    public void bulkTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);

        createUncachedObjects(100);
        waitForWrites();

        MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
        //        UpdateBulkRequest up = c
        c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 999, true, true);
        Map<String, Object> ret = c.runBulk();
        Thread.sleep(500);

        for (UncachedObject o : morphium.createQueryFor(UncachedObject.class).asList()) {
            assert (o.getCounter() == 999) : "Counter is " + o.getCounter();
        }

    }


    @Test
    public void incTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(100);

        MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addIncRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 1000, true, true);
        c.runBulk();
        Thread.sleep(1500);

        for (UncachedObject o : morphium.createQueryFor(UncachedObject.class).asList()) {
            assert (o.getCounter() > 1000) : "Counter is " + o.getCounter();
        }
    }


    @Test
    public void callbackTest() throws Exception {
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
            public void preUpdate(Morphium m, Class<? extends UncachedObject> cls, Enum updateType) {
                preUpdate = true;
            }

            @Override
            public void postUpdate(Morphium m, Class<? extends UncachedObject> cls, Enum updateType) {
                postUpdate = true;
            }
        };

        morphium.addListener(listener);
        preUpdate = postUpdate = preRemove = postRemove = false;
        incTest();
        Thread.sleep(1500);
        assert (preUpdate);
        assert (postUpdate);
        assert (!preRemove);
        assert (!postRemove);
        morphium.removeListener(listener);
    }

}
