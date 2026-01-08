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

            TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(),
                "UncachedObject not found",
                () -> morphium.createQueryFor(UncachedObject.class).get() != null);
            UncachedObject uc1 = morphium.createQueryFor(UncachedObject.class).get();

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
            TestUtils.waitForWrites(morphium, log);

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
            TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(),
                "Bulk set operation not persisted",
                () -> morphium.createQueryFor(UncachedObject.class).f("counter").eq(999).countAll() == 100);
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
            TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(),
                "Bulk inc operation not persisted",
                () -> morphium.createQueryFor(UncachedObject.class).f("counter").gte(1000).countAll() == 100);
            for (UncachedObject o : morphium.createQueryFor(UncachedObject.class).asList()) {
                if (o.getCounter() < 1000) {
                    log.error("Counter is < 1000!?");
                    morphium.reread(o);
                }
                assert (o.getCounter() >= 1000) : "Counter is " + o.getCounter() + " - Total number: " + TestUtils.countUC(morphium) + " >= 1000: " + morphium.createQueryFor(UncachedObject.class).f("counter").gte(1000).countAll();
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
            TestUtils.waitForConditionToBecomeTrue(3000, "Bulk operation callbacks not triggered",
                () -> preUpdate && postUpdate);
            assert (preUpdate);
            assert (postUpdate);
            assert (!preRemove);
            assert (!postRemove);
            morphium.removeListener(listener);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void bulkTestReturnCounts(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForWrites(morphium, log);

            // Create test data
            createUncachedObjects(morphium, 10);
            TestUtils.waitForWrites(morphium, log);

            // Create bulk context and add various operations
            MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);

            // Add 5 inserts
            for (int i = 0; i < 5; i++) {
                c.addInsertRequest(Arrays.asList(new UncachedObject("bulk_insert_" + i, 1000 + i)));
            }

            // Add updates (should match 10 existing documents)
            c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").lt(100), "counter", 500, false, true);

            // Add another update with upsert
            c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("strValue").eq("nonexistent"), "counter", 999, true, false);

            // Add deletes (should delete documents with counter=500)
            c.addDeleteRequest(morphium.createQueryFor(UncachedObject.class).f("counter").eq(500), true);

            // Execute bulk operations
            Map<String, Object> ret = c.runBulk();

            log.info("Bulk operation results: " + ret);

            // Verify return values are present and correct
assert ret != null : "Bulk operation should return results";
            assert ret.containsKey("num_inserted") : "Result should contain num_inserted";
            assert ret.containsKey("num_matched") : "Result should contain num_matched";
            assert ret.containsKey("num_modified") : "Result should contain num_modified";
            assert ret.containsKey("num_deleted") : "Result should contain num_deleted";
            assert ret.containsKey("num_upserts") : "Result should contain num_upserts";

            int inserted = ((Number) ret.get("num_inserted")).intValue();
            int matched = ((Number) ret.get("num_matched")).intValue();
            int modified = ((Number) ret.get("num_modified")).intValue();
            int deleted = ((Number) ret.get("num_deleted")).intValue();
            int upserts = ((Number) ret.get("num_upserts")).intValue();

            log.info(String.format("Inserted: %d, Matched: %d, Modified: %d, Deleted: %d, Upserts: %d",
                                   inserted, matched, modified, deleted, upserts));

            // Verify counts
assert inserted == 5 : "Should have inserted 5 documents, got: " + inserted;
assert matched >= 10 : "Should have matched at least 10 documents, got: " + matched;
assert modified >= 10 : "Should have modified at least 10 documents, got: " + modified;
assert deleted >= 10 : "Should have deleted at least 10 documents, got: " + deleted;
assert upserts == 1 : "Should have 1 upsert, got: " + upserts;

            // Check upserted IDs
            if (upserts > 0) {
                assert ret.containsKey("upsertedIds") : "Result should contain upsertedIds when upserts occurred";
                log.info("Upserted IDs: " + ret.get("upsertedIds"));
            }

            log.info("✓ Bulk operation return counts verified successfully");
        }
    }

}
