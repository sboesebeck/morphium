package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class DistinctTest extends MultiDriverTestBase {

    @NoCache
    @Entity
    public static class DistinctTestEntity {
        @Id
        private MorphiumId id;
        private String accountId;
        private String invoiceId;
        private String billingMode;
        private int counter;

        public DistinctTestEntity() {}

        public DistinctTestEntity(String accountId, String invoiceId, String billingMode, int counter) {
            this.accountId = accountId;
            this.invoiceId = invoiceId;
            this.billingMode = billingMode;
            this.counter = counter;
        }

        public MorphiumId getId() { return id; }
        public void setId(MorphiumId id) { this.id = id; }
        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }
        public String getInvoiceId() { return invoiceId; }
        public void setInvoiceId(String invoiceId) { this.invoiceId = invoiceId; }
        public String getBillingMode() { return billingMode; }
        public void setBillingMode(String billingMode) { this.billingMode = billingMode; }
        public int getCounter() { return counter; }
        public void setCounter(int counter) { this.counter = counter; }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctTest(Morphium morphium) {
        try (morphium) {
            createUncachedObjects(morphium, 100);

            List lst = morphium.createQueryFor(UncachedObject.class).distinct("counter");
            assert (lst.size() == 100);
            lst = morphium.createQueryFor(UncachedObject.class).distinct("str_value");
            assert (lst.size() == 1);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctWithFieldNameTranslation(Morphium morphium) {
        try (morphium) {
            createUncachedObjects(morphium, 100);

            // Test that Java field name "strValue" is translated to "str_value"
            List lst = morphium.createQueryFor(UncachedObject.class).distinct("strValue");
            assertEquals(1, lst.size(), "distinct with Java field name should translate to MongoDB field name");

            // Test with MongoDB field name directly — should also work
            lst = morphium.createQueryFor(UncachedObject.class).distinct("str_value");
            assertEquals(1, lst.size(), "distinct with MongoDB field name should work directly");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctWithNullFilter(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(DistinctTestEntity.class);
            TestUtils.waitForConditionToBecomeTrue(5000, "Collection drop", () -> morphium.createQueryFor(DistinctTestEntity.class).countAll() == 0);

            // Create test data: some with invoiceId=null, some with invoiceId set
            List<DistinctTestEntity> entities = new ArrayList<>();
            entities.add(new DistinctTestEntity("acc1", null, "BILL", 1));
            entities.add(new DistinctTestEntity("acc2", null, "BILL", 2));
            entities.add(new DistinctTestEntity("acc3", null, "NONE", 3));
            entities.add(new DistinctTestEntity("acc1", "inv1", "BILL", 4));  // has invoice
            entities.add(new DistinctTestEntity("acc4", null, "BILL", 5));
            morphium.storeList(entities);
            TestUtils.waitForConditionToBecomeTrue(5000, "Store", () -> morphium.createQueryFor(DistinctTestEntity.class).countAll() == 5);

            // distinct account_id where invoice_id is null
            List<?> result = morphium.createQueryFor(DistinctTestEntity.class)
                    .f("invoiceId").eq(null)
                    .distinct("accountId");

            assertNotNull(result, "distinct result should not be null");
            // acc1, acc2, acc3, acc4 have null invoiceId (acc1 appears twice but distinct)
            assertEquals(4, result.size(), "Should find 4 distinct accounts with null invoiceId");

            morphium.dropCollection(DistinctTestEntity.class);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctWithNullAndNeFilter(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(DistinctTestEntity.class);
            TestUtils.waitForConditionToBecomeTrue(5000, "Collection drop", () -> morphium.createQueryFor(DistinctTestEntity.class).countAll() == 0);

            // Replicate the exact scenario from the bug report:
            // query.f("invoice_id").eq(null).f("billing_mode").ne("NONE").distinct("account_id")
            List<DistinctTestEntity> entities = new ArrayList<>();
            entities.add(new DistinctTestEntity("acc1", null, "BILL", 1));     // match: null invoice, mode != NONE
            entities.add(new DistinctTestEntity("acc2", null, "BILL", 2));     // match
            entities.add(new DistinctTestEntity("acc3", null, "NONE", 3));     // no match: mode == NONE
            entities.add(new DistinctTestEntity("acc1", "inv1", "BILL", 4));   // no match: has invoice
            entities.add(new DistinctTestEntity("acc4", null, "BILL", 5));     // match
            entities.add(new DistinctTestEntity("acc5", null, "PREPAID", 6));  // match
            morphium.storeList(entities);
            TestUtils.waitForConditionToBecomeTrue(5000, "Store", () -> morphium.createQueryFor(DistinctTestEntity.class).countAll() == 6);

            // Combined filter: invoiceId == null AND billingMode != "NONE"
            List<?> result = morphium.createQueryFor(DistinctTestEntity.class)
                    .f("invoiceId").eq(null)
                    .f("billingMode").ne("NONE")
                    .distinct("accountId");

            assertNotNull(result, "distinct result should not be null");
            // acc1 (BILL, null invoice), acc2 (BILL, null invoice), acc4 (BILL, null invoice), acc5 (PREPAID, null invoice)
            assertEquals(4, result.size(), "Should find 4 distinct accounts with null invoiceId and billingMode != NONE");
            assertTrue(result.contains("acc1"));
            assertTrue(result.contains("acc2"));
            assertTrue(result.contains("acc4"));
            assertTrue(result.contains("acc5"));

            morphium.dropCollection(DistinctTestEntity.class);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctWithNeFilterOnly(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(DistinctTestEntity.class);
            TestUtils.waitForConditionToBecomeTrue(5000, "Collection drop", () -> morphium.createQueryFor(DistinctTestEntity.class).countAll() == 0);

            List<DistinctTestEntity> entities = new ArrayList<>();
            entities.add(new DistinctTestEntity("acc1", null, "BILL", 1));
            entities.add(new DistinctTestEntity("acc2", null, "NONE", 2));
            entities.add(new DistinctTestEntity("acc3", null, "PREPAID", 3));
            morphium.storeList(entities);
            TestUtils.waitForConditionToBecomeTrue(5000, "Store", () -> morphium.createQueryFor(DistinctTestEntity.class).countAll() == 3);

            List<?> result = morphium.createQueryFor(DistinctTestEntity.class)
                    .f("billingMode").ne("NONE")
                    .distinct("accountId");

            assertNotNull(result);
            assertEquals(2, result.size(), "Should find 2 distinct accounts with billingMode != NONE");
            assertTrue(result.contains("acc1"));
            assertTrue(result.contains("acc3"));

            morphium.dropCollection(DistinctTestEntity.class);
        }
    }
}
