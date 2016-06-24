package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.TypeMapper;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Created by stephan on 26.11.15.
 */
public class MorphiumTest extends MongoTest {

    @Test
    public void testGetConfig() throws Exception {
        assert (morphium.getConfig() != null);
        assert (morphium.getConfig().getDatabase() != null);
        assert (morphium.getConfig().getDatabase().equals("morphium_test"));
    }

    @Test
    public void testGetAsyncOperationsThreadPool() throws Exception {
        assert (morphium.getAsyncOperationsThreadPool() != null);
    }

    @Test(expected = RuntimeException.class)
    public void testSetConfig() throws Exception {
        morphium.setConfig(new MorphiumConfig());
    }

    @Test
    public void testRegisterTypeMapper() throws Exception {
        TypeMapper m = new TypeMapper() {
            @Override
            public Object marshall(Object o) {
                return null;
            }

            @Override
            public Object unmarshall(Object d) {
                return null;
            }
        };
        morphium.registerTypeMapper(UncachedObject.class, m);
        assert (morphium.getMapper().marshall(new UncachedObject()).containsKey("value"));

        morphium.deregisterTypeMapper(UncachedObject.class);
        assert (!morphium.getMapper().marshall(new UncachedObject()).containsKey("value"));

    }

    @Test
    public void testGetCache() throws Exception {
        assert (morphium.getCache() != null);
    }

    @Test
    public void testAddListener() throws Exception {
        MorphiumStorageAdapter lst = new MorphiumStorageAdapter() {
        };
        morphium.addListener(lst);
        morphium.removeListener(lst);
    }

    @Test
    public void testGetDriver() throws Exception {
        assert (morphium.getDriver() != null);
    }

    @Test
    public void testFindByTemplate() throws Exception {
        createUncachedObjects(100);
        UncachedObject o = new UncachedObject();
        o.setCounter(50);
        List<UncachedObject> uc = morphium.findByTemplate(o, "counter");
        assert (uc.size() == 1);
        assert (uc.get(0).getCounter() == 50);
    }

    @Test
    public void testUnset() throws Exception {
        createUncachedObjects(100);
        UncachedObject uc = morphium.findByField(UncachedObject.class, "counter", 50).get(0);
        morphium.unset(uc, UncachedObject.Fields.value);
        uc = morphium.reread(uc);
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnset1() throws Exception {
        createUncachedObjects(100);
        UncachedObject uc = morphium.findByField(UncachedObject.class, UncachedObject.Fields.counter, 50).get(0);
        morphium.unset(uc, "value");
        uc = morphium.reread(uc);
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnset2() throws Exception {
        createUncachedObjects(100);
        UncachedObject uc = morphium.findByField(UncachedObject.class, UncachedObject.Fields.counter, 50).get(0);
        morphium.unset(uc, UncachedObject.Fields.value, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
            }
        });
        waitForAsyncOperationToStart(1000000);
        Thread.sleep(1000);
        uc = morphium.reread(uc);
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnset3() throws Exception {
        createUncachedObjects(100);
        UncachedObject uc = morphium.findByField(UncachedObject.class, UncachedObject.Fields.counter, 50).get(0);
        morphium.unset(uc, "uncached_object", UncachedObject.Fields.value);
        uc = morphium.reread(uc);
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
        morphium.unsetQ(q, "value");
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ1() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
        morphium.unsetQ(q, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        }, "value");
        waitForAsyncOperationToStart(100000);
        Thread.sleep(1000);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ2() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").lt(50);
        morphium.unsetQ(q, true, "value");
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ3() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
        morphium.unsetQ(q, UncachedObject.Fields.value);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);

    }

    @Test
    public void testUnsetQ4() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").lt(50);
        morphium.unsetQ(q, true, UncachedObject.Fields.value);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ5() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
        morphium.unsetQ(q, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        }, UncachedObject.Fields.value);
        Thread.sleep(500);

        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ6() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").eq(50);
        morphium.unsetQ(q, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        }, "value");
        Thread.sleep(500);

        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ7() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").lt(50);
        morphium.unsetQ(q, true, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        }, UncachedObject.Fields.value);
        Thread.sleep(500);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testEnsureIndicesFor() throws Exception {
        morphium.ensureIndicesFor(UncachedObject.class);
    }

    @Test
    public void testEnsureIndicesFor1() throws Exception {
        morphium.ensureIndicesFor(UncachedObject.class, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        });
    }

    @Test
    public void testEnsureIndicesFor2() throws Exception {
        morphium.ensureIndicesFor(UncachedObject.class, "different_coll");
        assert (morphium.getDriver().exists("morphium_test", "different_coll"));
    }

    @Test
    public void testEnsureIndicesFor3() throws Exception {
        Map<String, Object> idx = Utils.getMap("counter", 1);
        morphium.ensureIndex(UncachedObject.class, idx);
    }

    @Test
    public void testConvertToCapped() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        while (q.countAll() != 100) {
            log.info("Count is " + q.countAll());
        }
        morphium.convertToCapped(UncachedObject.class, 2500, null);
        Thread.sleep(500);
        for (int i = 0; i < 500; i++) {
            if (i % 100 == 0) {
                log.info("Sored " + i);
            }
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 100);
            uc.setValue("nothing");
            morphium.store(uc);
        }
        Thread.sleep(500);
        assert (q.countAll() >= 30 && q.countAll() < 40) : "Count is " + q.countAll();

    }

    @Test
    public void testExecCommand() throws Exception {
        morphium.execCommand(Utils.getMap("dbstats", 1));
    }

    @Test
    public void testExecCommand1() throws Exception {
        morphium.execCommand("isMaster");
    }

    @Test
    public void testEnsureCapped() throws Exception {
        morphium.ensureCapped(UncachedObject.class);
    }

    @Test
    public void testEnsureCapped1() throws Exception {
        morphium.ensureCapped(UncachedObject.class, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        });
    }

    @Test
    public void testSimplifyQueryObject() throws Exception {
        morphium.simplifyQueryObject(morphium.createQueryFor(UncachedObject.class).f("counter").eq(100).toQueryObject());
    }

}