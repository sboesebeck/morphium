package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class QueryUpdateOperatorsTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSet(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).set(UncachedObject.Fields.strValue, "changed", false, false, null);
        TestUtils.waitForConditionToBecomeTrue(5000, "Set not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetEnum(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).setEnum(m, false, false);
        TestUtils.waitForConditionToBecomeTrue(5000, "SetEnum not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetEnum2(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(3).setEnum(m, false, true);
        TestUtils.waitForConditionToBecomeTrue(5000, "SetEnum multiple not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").countAll() == 3);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetEnum3(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(1000).f(UncachedObject.Fields.counter).lt(1002).setEnum(m, true, true);
        TestUtils.waitForConditionToBecomeTrue(5000, "Upserted setEnum not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetEnumAsync(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        AtomicLong cnt = new AtomicLong(0);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(2).setEnum(m, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                log.info("success!");
                log.info(Utils.toJsonString(param));
                cnt.incrementAndGet();
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
            }
        });

        TestUtils.waitForConditionToBecomeTrue(10000, "Async setEnum callback not called", () -> cnt.get() > 0);
        TestUtils.waitForConditionToBecomeTrue(5000, "Async setEnum not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetUpsert(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        TestUtils.waitForWrites(morphium, log);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(10002).set(UncachedObject.Fields.strValue, "new", true, true, null);
        // Wait for the upserted object to be queryable in replica set
        TestUtils.waitForConditionToBecomeTrue(15000, "Upserted object not queryable",
            () -> !morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList().isEmpty());
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList();
        assertEquals(lst.size(), 1);
        assertTrue((lst.get(0).getCounter() == 10002));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSet2(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Map<String, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue.name(), "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(2).set(m);
        TestUtils.waitForConditionToBecomeTrue(5000, "Set via map not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSet3(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Map<String, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue.name(), "new");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(10002).set(m, true, true, null);
        TestUtils.waitForConditionToBecomeTrue(5000, "Upserted object not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").countAll() == 1);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList();
        assertEquals(1, lst.size());
        assertEquals(10002, lst.get(0).getCounter());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testPush(Morphium morphium) throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        uc.setIntData(new int[0]); // Initialize with empty array for push to work
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().connectionSettings().getMaxWaitTime(), "Did not store?", () -> morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.morphiumId).eq(uc.getMorphiumId())
            .push(UncachedObject.Fields.intData, 42);
        TestUtils.waitForConditionToBecomeTrue(5000, "Push not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).f(UncachedObject.Fields.intData).eq(42).countAll() == 1);
        morphium.reread(uc);
        assertNotNull(uc.getIntData());
        assertEquals(42, uc.getIntData()[0]);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testPushAll(Morphium morphium) throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        uc.setIntData(new int[0]); // Initialize with empty array for pushAll to work
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().connectionSettings().getMaxWaitTime(), "Did not store?", () -> morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        List<Integer> lst = Arrays.asList(42, 123);
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pushAll(UncachedObject.Fields.intData, lst);
        TestUtils.waitForConditionToBecomeTrue(5000, "PushAll not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).f(UncachedObject.Fields.intData).eq(123).countAll() == 1);
        morphium.reread(uc);
        assertNotNull(uc.getIntData());
        assertEquals(42, uc.getIntData()[0]);
        assertEquals(123, uc.getIntData()[1]);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testPull(Morphium morphium) throws Exception {
        UncachedObject uc = new UncachedObject("value", 1021);
        uc.setIntData(new int[] {12, 23, 52, 42});
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().connectionSettings().getMaxWaitTime(), "Did not store?", () -> morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pull(UncachedObject.Fields.intData, 12, false, false, null);
        TestUtils.waitForConditionToBecomeTrue(5000, "Pull not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).f(UncachedObject.Fields.intData).eq(12).countAll() == 0);
        morphium.reread(uc);
        assertEquals(3, uc.getIntData().length);
        assertEquals(23, uc.getIntData()[0]);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testInc(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .inc(UncachedObject.Fields.counter, 100);
        TestUtils.waitForConditionToBecomeTrue(5000, "Inc not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gte(100).countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testInc2(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .inc(UncachedObject.Fields.counter, 100, false, true);
        TestUtils.waitForConditionToBecomeTrue(5000, "Multiple inc not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gte(100).countAll() == 5);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testInc3(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .inc(UncachedObject.Fields.dval, 0.2, false, true);
        TestUtils.waitForConditionToBecomeTrue(5000, "Inc on dval not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.dval).eq(0.2).countAll() == 5);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testIncAsync(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        AtomicInteger ai = new AtomicInteger(0);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .inc(UncachedObject.Fields.dval, 0.2, false, true, new AsyncCallbackAdapter<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    ai.incrementAndGet();
                }
            });

        TestUtils.waitForConditionToBecomeTrue(10000, "Async inc callback not called", () -> ai.get() == 1);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Inc operation not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.dval).eq(0.2).countAll() == 5);
        long cnt = morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.dval).eq(0.2).countAll();
        assertNotEquals(0, cnt);
        assertEquals(5, cnt);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDec(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .dec(UncachedObject.Fields.counter, 100);
        TestUtils.waitForConditionToBecomeTrue(5000, "Dec not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(0).countAll() == 1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDec2(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .dec(UncachedObject.Fields.counter, 100, false, true);
        TestUtils.waitForConditionToBecomeTrue(5000, "Multiple dec not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(0).countAll() == 5);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDec3(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .dec(UncachedObject.Fields.dval, 0.2, false, true);

        TestUtils.waitForConditionToBecomeTrue((long) morphium.getConfig().connectionSettings().getMaxWaitTime(), "took too long", () -> morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.dval).lt(0).countAll() >= 4);

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDecAsync(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        TestUtils.waitForConditionToBecomeTrue((long) morphium.getConfig().connectionSettings().getMaxWaitTime(), "Objects not stored", () -> TestUtils.countUC(morphium) >= 10);
        AtomicInteger ai = new AtomicInteger(0);
        morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.counter).lt(5)
            .dec(UncachedObject.Fields.dval, 0.2, false, true, new AsyncCallbackAdapter<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    ai.incrementAndGet();
                }
            });

        TestUtils.waitForConditionToBecomeTrue(10000, "Async dec callback not called", () -> ai.get() == 1);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "Dec operation not visible", () ->
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.dval).eq(-0.2).countAll() == 5);
        long cnt = morphium.createQueryFor(UncachedObject.class)
            .f(UncachedObject.Fields.dval).eq(-0.2).countAll();
        assertNotEquals(0, cnt);
        assertEquals(5, cnt);
    }

    private de.caluga.test.mongo.suite.data.ListContainer storeListContainer(Morphium morphium) {
        de.caluga.test.mongo.suite.data.ListContainer lc = new de.caluga.test.mongo.suite.data.ListContainer();
        lc.setName("arrayFilters");
        lc.addLong(85);
        lc.addLong(92);
        lc.addLong(90);
        morphium.store(lc);
        return lc;
    }

    private String longListPath(Morphium morphium) {
        return morphium.getARHelper().getMongoFieldName(de.caluga.test.mongo.suite.data.ListContainer.class, "longList") + ".$[elem]";
    }

    private Query<de.caluga.test.mongo.suite.data.ListContainer> lcQuery(Morphium morphium) {
        return morphium.createQueryFor(de.caluga.test.mongo.suite.data.ListContainer.class)
               .f(de.caluga.test.mongo.suite.data.ListContainer.Fields.name).eq("arrayFilters");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetWithArrayFilters(Morphium morphium) throws Exception {
        try (morphium) {
            storeListContainer(morphium);
            Query<de.caluga.test.mongo.suite.data.ListContainer> q = lcQuery(morphium)
                .setArrayFilters(de.caluga.morphium.driver.Doc.of("elem", de.caluga.morphium.driver.Doc.of("$gte", 90)));
            q.set(longListPath(morphium), 100L, false, false);
            TestUtils.waitForConditionToBecomeTrue(5000, "arrayFilters $set not applied",
                () -> {
                    var r = lcQuery(morphium).get();
                    return r != null && List.of(85L, 100L, 100L).equals(r.getLongList());
                });
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testIncWithArrayFilters(Morphium morphium) throws Exception {
        try (morphium) {
            storeListContainer(morphium);
            Query<de.caluga.test.mongo.suite.data.ListContainer> q = lcQuery(morphium)
                .setArrayFilters(de.caluga.morphium.driver.Doc.of("elem", de.caluga.morphium.driver.Doc.of("$gte", 90)));
            q.inc(longListPath(morphium), 5, false, false);
            TestUtils.waitForConditionToBecomeTrue(5000, "arrayFilters $inc not applied",
                () -> {
                    var r = lcQuery(morphium).get();
                    return r != null && List.of(85L, 97L, 95L).equals(r.getLongList());
                });
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testUnsetWithArrayFilters(Morphium morphium) throws Exception {
        try (morphium) {
            storeListContainer(morphium);
            Query<de.caluga.test.mongo.suite.data.ListContainer> q = lcQuery(morphium)
                .setArrayFilters(de.caluga.morphium.driver.Doc.of("elem", de.caluga.morphium.driver.Doc.of("$gte", 90)));
            q.unset(longListPath(morphium));
            // $unset on an array element leaves null in place (MongoDB semantics)
            TestUtils.waitForConditionToBecomeTrue(5000, "arrayFilters $unset not applied",
                () -> {
                    List<Long> l = lcQuery(morphium).get().getLongList();
                    return l.size() == 3 && l.get(0) == 85L && l.get(1) == null && l.get(2) == null;
                });
        }
    }
}
