package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.TypeMapper;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

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
        assert (morphium.getMapper().marshall(new UncachedObject()) == null);

        morphium.deregisterTypeMapper(UncachedObject.class);
        assert (morphium.getMapper().marshall(new UncachedObject()) != null);

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
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("count").eq(50);
        morphium.unsetQ(q, "value");
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ1() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("count").eq(50);
        morphium.unsetQ(q, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        }, "value");
        waitForAsyncOperationToStart(100000);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ2() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("count").lt(50);
        morphium.unsetQ(q, true, "value");
        UncachedObject uc = q.get();
        assert (uc.getValue() == null);
    }

    @Test
    public void testUnsetQ3() throws Exception {

    }

    @Test
    public void testUnsetQ4() throws Exception {

    }

    @Test
    public void testUnsetQ5() throws Exception {

    }

    @Test
    public void testUnsetQ6() throws Exception {

    }

    @Test
    public void testUnsetQ7() throws Exception {

    }

    @Test
    public void testEnsureIndicesFor() throws Exception {

    }

    @Test
    public void testEnsureIndicesFor1() throws Exception {

    }

    @Test
    public void testEnsureIndicesFor2() throws Exception {

    }

    @Test
    public void testEnsureIndicesFor3() throws Exception {

    }

    @Test
    public void testConvertToCapped() throws Exception {

    }

    @Test
    public void testConvertToCapped1() throws Exception {

    }

    @Test
    public void testExecCommand() throws Exception {

    }

    @Test
    public void testExecCommand1() throws Exception {

    }

    @Test
    public void testEnsureCapped() throws Exception {

    }

    @Test
    public void testEnsureCapped1() throws Exception {

    }

    @Test
    public void testSimplifyQueryObject() throws Exception {

    }

    @Test
    public void testSet() throws Exception {

    }

    @Test
    public void testSet1() throws Exception {

    }

    @Test
    public void testSet2() throws Exception {

    }

    @Test
    public void testSet3() throws Exception {

    }

    @Test
    public void testSetEnum() throws Exception {

    }

    @Test
    public void testPush() throws Exception {

    }

    @Test
    public void testPull() throws Exception {

    }

    @Test
    public void testPush1() throws Exception {

    }

    @Test
    public void testPull1() throws Exception {

    }

    @Test
    public void testPush2() throws Exception {

    }

    @Test
    public void testPull2() throws Exception {

    }

    @Test
    public void testPushAll() throws Exception {

    }

    @Test
    public void testPullAll() throws Exception {

    }

    @Test
    public void testPush3() throws Exception {

    }

    @Test
    public void testPush4() throws Exception {

    }

    @Test
    public void testPull3() throws Exception {

    }

    @Test
    public void testPull4() throws Exception {

    }

    @Test
    public void testPushAll1() throws Exception {

    }

    @Test
    public void testPushAll2() throws Exception {

    }

    @Test
    public void testSet4() throws Exception {

    }

    @Test
    public void testSet5() throws Exception {

    }

    @Test
    public void testPullAll1() throws Exception {

    }

    @Test
    public void testSet6() throws Exception {

    }

    @Test
    public void testSet7() throws Exception {

    }

    @Test
    public void testSet8() throws Exception {

    }

    @Test
    public void testSet9() throws Exception {

    }

    @Test
    public void testSet10() throws Exception {

    }

    @Test
    public void testSet11() throws Exception {

    }

    @Test
    public void testSet12() throws Exception {

    }

    @Test
    public void testSet13() throws Exception {

    }

    @Test
    public void testSet14() throws Exception {

    }

    @Test
    public void testSet15() throws Exception {

    }

    @Test
    public void testDec() throws Exception {

    }

    @Test
    public void testDec1() throws Exception {

    }

    @Test
    public void testDec2() throws Exception {

    }

    @Test
    public void testDec3() throws Exception {

    }

    @Test
    public void testDec4() throws Exception {

    }

    @Test
    public void testDec5() throws Exception {

    }

    @Test
    public void testDec6() throws Exception {

    }

    @Test
    public void testDec7() throws Exception {

    }

    @Test
    public void testDec8() throws Exception {

    }

    @Test
    public void testDec9() throws Exception {

    }

    @Test
    public void testDec10() throws Exception {

    }

    @Test
    public void testDec11() throws Exception {

    }

    @Test
    public void testDec12() throws Exception {

    }

    @Test
    public void testDec13() throws Exception {

    }

    @Test
    public void testDec14() throws Exception {

    }

    @Test
    public void testDec15() throws Exception {

    }

    @Test
    public void testInc() throws Exception {

    }

    @Test
    public void testInc1() throws Exception {

    }

    @Test
    public void testInc2() throws Exception {

    }

    @Test
    public void testInc3() throws Exception {

    }

    @Test
    public void testInc4() throws Exception {

    }

    @Test
    public void testInc5() throws Exception {

    }

    @Test
    public void testInc6() throws Exception {

    }

    @Test
    public void testInc7() throws Exception {

    }

    @Test
    public void testInc8() throws Exception {

    }

    @Test
    public void testInc9() throws Exception {

    }

    @Test
    public void testInc10() throws Exception {

    }

    @Test
    public void testInc11() throws Exception {

    }

    @Test
    public void testInc12() throws Exception {

    }

    @Test
    public void testInc13() throws Exception {

    }

    @Test
    public void testInc14() throws Exception {

    }

    @Test
    public void testInc15() throws Exception {

    }

    @Test
    public void testInc16() throws Exception {

    }

    @Test
    public void testInc17() throws Exception {

    }

    @Test
    public void testInc18() throws Exception {

    }

    @Test
    public void testInc19() throws Exception {

    }

    @Test
    public void testInc20() throws Exception {

    }

    @Test
    public void testGetWriterForClass() throws Exception {

    }

    @Test
    public void testDec16() throws Exception {

    }

    @Test
    public void testDec17() throws Exception {

    }

    @Test
    public void testDec18() throws Exception {

    }

    @Test
    public void testDec19() throws Exception {

    }

    @Test
    public void testInc21() throws Exception {

    }

    @Test
    public void testInc22() throws Exception {

    }

    @Test
    public void testInc23() throws Exception {

    }

    @Test
    public void testInc24() throws Exception {

    }

    @Test
    public void testInc25() throws Exception {

    }

    @Test
    public void testInc26() throws Exception {

    }

    @Test
    public void testInc27() throws Exception {

    }

    @Test
    public void testInc28() throws Exception {

    }

    @Test
    public void testInc29() throws Exception {

    }

    @Test
    public void testInc30() throws Exception {

    }

    @Test
    public void testInc31() throws Exception {

    }

    @Test
    public void testInc32() throws Exception {

    }

    @Test
    public void testDelete() throws Exception {

    }

    @Test
    public void testDelete1() throws Exception {

    }

    @Test
    public void testDelete2() throws Exception {

    }

    @Test
    public void testInc33() throws Exception {

    }

    @Test
    public void testUpdateUsingFields() throws Exception {

    }

    @Test
    public void testUpdateUsingFields1() throws Exception {

    }

    @Test
    public void testUpdateUsingFields2() throws Exception {

    }

    @Test
    public void testGetMapper() throws Exception {

    }

    @Test
    public void testGetARHelper() throws Exception {

    }

    @Test
    public void testReread() throws Exception {

    }

    @Test
    public void testReread1() throws Exception {

    }

    @Test
    public void testFirePreStore() throws Exception {

    }

    @Test
    public void testFirePostStore() throws Exception {

    }

    @Test
    public void testFirePreDrop() throws Exception {

    }

    @Test
    public void testFirePostStore1() throws Exception {

    }

    @Test
    public void testFirePostRemove() throws Exception {

    }

    @Test
    public void testFirePostLoad() throws Exception {

    }

    @Test
    public void testFirePreStore1() throws Exception {

    }

    @Test
    public void testFirePreRemove() throws Exception {

    }

    @Test
    public void testFirePreRemove1() throws Exception {

    }

    @Test
    public void testFirePostDropEvent() throws Exception {

    }

    @Test
    public void testFirePostUpdateEvent() throws Exception {

    }

    @Test
    public void testFirePreUpdateEvent() throws Exception {

    }

    @Test
    public void testFirePostRemoveEvent() throws Exception {

    }

    @Test
    public void testFirePostRemoveEvent1() throws Exception {

    }

    @Test
    public void testFirePreRemoveEvent() throws Exception {

    }

    @Test
    public void testDeReference() throws Exception {

    }

    @Test
    public void testFirePostLoadEvent() throws Exception {

    }

    @Test
    public void testGetCurrentRSState() throws Exception {

    }

    @Test
    public void testIsReplicaSet() throws Exception {

    }

    @Test
    public void testGetReadPreferenceForClass() throws Exception {

    }

    @Test
    public void testCreateBulkRequestContext() throws Exception {

    }

    @Test
    public void testCreateBulkRequestContext1() throws Exception {

    }

    @Test
    public void testGetWriteConcernForClass() throws Exception {

    }

    @Test
    public void testAddProfilingListener() throws Exception {

    }

    @Test
    public void testRemoveProfilingListener() throws Exception {

    }

    @Test
    public void testFireProfilingWriteEvent() throws Exception {

    }

    @Test
    public void testFireProfilingReadEvent() throws Exception {

    }

    @Test
    public void testClearCollection() throws Exception {

    }

    @Test
    public void testClearCollection1() throws Exception {

    }

    @Test
    public void testClearCollectionOneByOne() throws Exception {

    }

    @Test
    public void testReadAll() throws Exception {

    }

    @Test
    public void testCreateQueryFor() throws Exception {

    }

    @Test
    public void testCreateQueryFor1() throws Exception {

    }

    @Test
    public void testFind() throws Exception {

    }

    @Test
    public void testDistinct() throws Exception {

    }

    @Test
    public void testDistinct1() throws Exception {

    }

    @Test
    public void testDistinct2() throws Exception {

    }

    @Test
    public void testDistinct3() throws Exception {

    }

    @Test
    public void testDistinct4() throws Exception {

    }

    @Test
    public void testGroup() throws Exception {

    }

    @Test
    public void testFindById() throws Exception {

    }

    @Test
    public void testFindByField() throws Exception {

    }

    @Test
    public void testFindByField1() throws Exception {

    }

    @Test
    public void testClearCachefor() throws Exception {

    }

    @Test
    public void testClearCacheforClassIfNecessary() throws Exception {

    }

    @Test
    public void testStoreNoCache() throws Exception {

    }

    @Test
    public void testStoreNoCache1() throws Exception {

    }

    @Test
    public void testStoreNoCache2() throws Exception {

    }

    @Test
    public void testStoreNoCache3() throws Exception {

    }

    @Test
    public void testStoreBuffered() throws Exception {

    }

    @Test
    public void testStoreBuffered1() throws Exception {

    }

    @Test
    public void testStoreBuffered2() throws Exception {

    }

    @Test
    public void testFlush() throws Exception {

    }

    @Test
    public void testGetId() throws Exception {

    }

    @Test
    public void testDropCollection() throws Exception {

    }

    @Test
    public void testDropCollection1() throws Exception {

    }

    @Test
    public void testDropCollection2() throws Exception {

    }

    @Test
    public void testEnsureIndex() throws Exception {

    }

    @Test
    public void testEnsureIndex1() throws Exception {

    }

    @Test
    public void testEnsureIndex2() throws Exception {

    }

    @Test
    public void testWriteBufferCount() throws Exception {

    }

    @Test
    public void testStore() throws Exception {

    }

    @Test
    public void testEnsureIndex3() throws Exception {

    }

    @Test
    public void testEnsureIndex4() throws Exception {

    }

    @Test
    public void testEnsureIndex5() throws Exception {

    }

    @Test
    public void testEnsureIndex6() throws Exception {

    }

    @Test
    public void testEnsureIndex7() throws Exception {

    }

    @Test
    public void testEnsureIndex8() throws Exception {

    }

    @Test
    public void testEnsureIndex9() throws Exception {

    }

    @Test
    public void testCreateIndexMapFrom() throws Exception {

    }

    @Test
    public void testEnsureIndex10() throws Exception {

    }

    @Test
    public void testEnsureIndex11() throws Exception {

    }

    @Test
    public void testStore1() throws Exception {

    }

    @Test
    public void testStore2() throws Exception {

    }

    @Test
    public void testStore3() throws Exception {

    }

    @Test
    public void testStore4() throws Exception {

    }

    @Test
    public void testStoreList() throws Exception {

    }

    @Test
    public void testStoreList1() throws Exception {

    }

    @Test
    public void testStoreList2() throws Exception {

    }

    @Test
    public void testStoreList3() throws Exception {

    }

    @Test
    public void testDelete3() throws Exception {

    }

    @Test
    public void testDelete4() throws Exception {

    }

    @Test
    public void testPushPull() throws Exception {

    }

    @Test
    public void testPushPullAll() throws Exception {

    }

    @Test
    public void testPullAll2() throws Exception {

    }

    @Test
    public void testDelete5() throws Exception {

    }

    @Test
    public void testDelete6() throws Exception {

    }

    @Test
    public void testDelete7() throws Exception {

    }

    @Test
    public void testDelete8() throws Exception {

    }

    @Test
    public void testGetStatistics() throws Exception {

    }

    @Test
    public void testResetStatistics() throws Exception {

    }

    @Test
    public void testGetStats() throws Exception {

    }

    @Test
    public void testAddShutdownListener() throws Exception {

    }

    @Test
    public void testRemoveShutdownListener() throws Exception {

    }

    @Test
    public void testClose() throws Exception {

    }

    @Test
    public void testCreateCamelCase() throws Exception {

    }

    @Test
    public void testCreateAggregator() throws Exception {

    }

    @Test
    public void testAggregate() throws Exception {

    }

    @Test
    public void testCreatePartiallyUpdateableEntity() throws Exception {

    }

    @Test
    public void testCreateLazyLoadedEntity() throws Exception {

    }

    @Test
    public void testCreateMongoField() throws Exception {

    }

    @Test
    public void testGetWriteBufferCount() throws Exception {

    }

    @Test
    public void testGetBufferedWriterBufferCount() throws Exception {

    }

    @Test
    public void testGetAsyncWriterBufferCount() throws Exception {

    }

    @Test
    public void testGetWriterBufferCount() throws Exception {

    }

    @Test
    public void testDisableAutoValuesForThread() throws Exception {

    }

    @Test
    public void testEnableAutoValuesForThread() throws Exception {

    }

    @Test
    public void testIsAutoValuesEnabledForThread() throws Exception {

    }

    @Test
    public void testDisableReadCacheForThread() throws Exception {

    }

    @Test
    public void testEnableReadCacheForThread() throws Exception {

    }

    @Test
    public void testIsReadCacheEnabledForThread() throws Exception {

    }

    @Test
    public void testDisableWriteBufferForThread() throws Exception {

    }

    @Test
    public void testEnableWriteBufferForThread() throws Exception {

    }

    @Test
    public void testIsWriteBufferEnabledForThread() throws Exception {

    }

    @Test
    public void testDisableAsyncWritesForThread() throws Exception {

    }

    @Test
    public void testEnableAsyncWritesForThread() throws Exception {

    }

    @Test
    public void testIsAsyncWritesEnabledForThread() throws Exception {

    }

    @Test
    public void testQueueTask() throws Exception {

    }

    @Test
    public void testGetNumberOfAvailableThreads() throws Exception {

    }

    @Test
    public void testGetActiveThreads() throws Exception {

    }

    @Test
    public void testFireWouldDereference() throws Exception {

    }

    @Test
    public void testFireDidDereference() throws Exception {

    }

    @Test
    public void testAddDereferencingListener() throws Exception {

    }

    @Test
    public void testRemoveDerrferencingListener() throws Exception {

    }
}