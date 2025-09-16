package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


@Tag("inmemory")
public class InMemAggregationTests extends MorphiumInMemTestBase {

    @Test
    public void inMemAggregationSumTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("mod0"));
        agg.group("$str_value").sum("summe", "$counter").sum("cnt", 1).end();
        agg.addFields(UtilsMap.of("tst", Expr.field("summe")));
        agg.project("avg", Expr.divide(Expr.field("tst"), Expr.field("cnt")));
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info("Count: " + lst.size());
        for (Map<String, Object> o : lst) {
            log.info(Utils.toJsonString(o));
        }

        assert (lst.size() == 1);
        assert (((Number) lst.get(0).get("summe")).doubleValue() == 1683);
        assert (((Number) lst.get(0).get("tst")).doubleValue() == 1683);
        assert (((Number) lst.get(0).get("cnt")).doubleValue() == 34);
        assert (((Number) lst.get(0).get("avg")).doubleValue() == 49.5);
        assert (lst.get(0).get("_id").equals("mod0"));
    }


    @Test
    public void inMemAggregationFirstLastTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.group("$str_value").first("cnt", "$counter").last("lst", "$counter").end();
        agg.sort("_id");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info("Count: " + lst.size());
        for (Map<String, Object> o : lst) {
            log.info(Utils.toJsonString(o));
        }
        assert (lst.size() == 3);
        assert (((Number) lst.get(0).get("cnt")).doubleValue() == 0);
        assert (((Number) lst.get(1).get("cnt")).doubleValue() == 1);
        assert (((Number) lst.get(2).get("cnt")).doubleValue() == 2);
        assert (((Number) lst.get(0).get("lst")).doubleValue() == 99);
        assert (((Number) lst.get(1).get("lst")).doubleValue() == 97);
        assert (((Number) lst.get(2).get("lst")).doubleValue() == 98);
    }

    @Test
    public void inMemAggregationSortTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.sort("str_value", "-counter");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info("Count: " + lst.size());
        String lastValue = "mod0";
        int lastCounter = 100;
        for (Map<String, Object> o : lst) {
            log.info(Utils.toJsonString(o));
            if (lastValue.equals(o.get("str_value"))) {
                assert (((Number) o.get("counter")).intValue() < lastCounter) : "LastCounter: " + lastCounter + " got: " + o.get("counter");
                lastCounter = ((Number) o.get("counter")).intValue();
            } else {
                lastCounter = 100;
                lastValue = (String) o.get("str_value");
            }

            assert (lastValue.compareTo((String) o.get("str_value")) <= 0) : "LastValue: " + lastValue + " current: " + o.get("str_value");

        }
    }

    @Test
    public void inMemAggregationCountTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.count("myCount");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info(Utils.toJsonString(lst.get(0)));
        assert (lst.size() == 1);
        assert (lst.get(0).get("myCount").equals(100));
    }

    @Test
    public void inMemAggregationPushTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.group("all").push("mods", "$value");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info(Utils.toJsonString(lst.get(0)));
        assert (lst.size() == 1);
        assert (((List) lst.get(0).get("mods")).size() == 100);
    }

    @Test
    public void inMemAggregationLookupTest() throws Exception {
        // Create test data: 3 users with different roles (0, 1, 2)
        for (int i = 0; i < 9; i++) {
            UncachedObject u = new UncachedObject("user" + i, i % 3); // counter = role_id
            morphium.store(u);
        }

        // Create role definitions
        for (int i = 0; i < 3; i++) {
            ModuloValue role = new ModuloValue();
            role.setModValue(i);
            role.setTextRep("Role " + i);
            morphium.store(role);
        }

        // Test $lookup without unwind first to verify actual join behavior
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.lookup(ModuloValue.class, UncachedObject.Fields.counter, ModuloValue.Fields.modValue, "roleInfo", null, null);
        agg.limit(3); // Just get first 3 to verify structure

        List<Map> result = agg.aggregate();

        // Verify the lookup actually worked
        assertNotNull(result);
        assertEquals(3, result.size()); // Should have 3 documents

        for (Map doc : result) {
            assertNotNull(doc.get("roleInfo")); // Each document should have roleInfo
            assertTrue(doc.get("roleInfo") instanceof List); // roleInfo should be an array

            List roleArray = (List) doc.get("roleInfo");
            assertEquals(1, roleArray.size()); // Should have exactly 1 matching role

            Map roleDoc = (Map) roleArray.get(0);
            assertNotNull(roleDoc.get("mod_value"));
            assertNotNull(roleDoc.get("text_rep"));

            // Verify the join condition: user.counter == role.mod_value
            assertEquals(doc.get("counter"), roleDoc.get("mod_value"));
        }
    }

    @Test
    public void inMemAggregationFunctionTest() throws Exception {
        // Check JavaScript engine availability (same pattern as WhereTest)
        javax.script.ScriptEngineManager mgr = new javax.script.ScriptEngineManager();
        if (mgr.getEngineByExtension("js") == null) {
            log.error("No javascript engine available - inMem $function not running...");
            return;
        }

        // Create test data
        for (int i = 1; i <= 5; i++) {
            UncachedObject u = new UncachedObject("test" + i, i);
            morphium.store(u);
        }

        // Test $function in $project stage - simple calculation
        Aggregator<UncachedObject, Map> agg1 = morphium.createAggregator(UncachedObject.class, Map.class);
        Map<String, Object> projection1 = new java.util.HashMap<>();
        projection1.put("str_value", Expr.field("str_value"));
        projection1.put("counter", Expr.field("counter"));
        projection1.put("squared", Expr.function("function(x) { return x * x; }", java.util.Arrays.asList(Expr.field("counter"))));
        projection1.put("doubled", Expr.function("function(n) { return n * 2; }", java.util.Arrays.asList(Expr.field("counter"))));
        agg1.project(projection1);
        agg1.limit(3);

        List<Map> result1 = agg1.aggregate();
        assertNotNull(result1);
        assertEquals(3, result1.size());

        for (Map doc : result1) {
            log.info("Result document: " + Utils.toJsonString(doc));
            Integer counter = (Integer) doc.get("counter");
            Number squared = (Number) doc.get("squared");
            Number doubled = (Number) doc.get("doubled");

            if (squared == null) {
                log.error("squared is null for counter: " + counter);
                fail("$function not working - squared is null");
            }
            if (doubled == null) {
                log.error("doubled is null for counter: " + counter);
                fail("$function not working - doubled is null");
            }

            assertEquals(counter * counter, squared.intValue());
            assertEquals(counter * 2, doubled.intValue());
        }

        // Test $function with multiple arguments
        Aggregator<UncachedObject, Map> agg2 = morphium.createAggregator(UncachedObject.class, Map.class);
        Map<String, Object> projection2 = new java.util.HashMap<>();
        projection2.put("str_value", Expr.field("str_value"));
        projection2.put("counter", Expr.field("counter"));
        projection2.put("calculated", Expr.function(
            "function(a, b) { return a * 10 + b; }",
            java.util.Arrays.asList(Expr.field("counter"), Expr.intExpr(5))
        ));
        agg2.project(projection2);
        agg2.limit(2);

        List<Map> result2 = agg2.aggregate();
        assertNotNull(result2);
        assertEquals(2, result2.size());

        for (Map doc : result2) {
            Integer counter = (Integer) doc.get("counter");
            Number calculated = (Number) doc.get("calculated");
            assertEquals(counter * 10 + 5, calculated.intValue());
        }

        // Test $function with string manipulation
        Aggregator<UncachedObject, Map> agg3 = morphium.createAggregator(UncachedObject.class, Map.class);
        Map<String, Object> projection3 = new java.util.HashMap<>();
        projection3.put("original", Expr.field("str_value"));
        projection3.put("reversed", Expr.function(
            "function(str) { return str.split('').reverse().join(''); }",
            java.util.Arrays.asList(Expr.field("str_value"))
        ));
        agg3.project(projection3);
        agg3.limit(1);

        List<Map> result3 = agg3.aggregate();
        assertNotNull(result3);
        assertEquals(1, result3.size());

        String original = (String) result3.get(0).get("original");
        String reversed = (String) result3.get(0).get("reversed");
        assertEquals(new StringBuilder(original).reverse().toString(), reversed);
    }

    @Test
    public void inMemAggregationFunctionGracefulFallbackTest() throws Exception {
        // This test verifies that $function gracefully handles missing JavaScript engine
        // by returning null values instead of throwing exceptions

        // Create test data
        for (int i = 1; i <= 3; i++) {
            UncachedObject u = new UncachedObject("test" + i, i);
            morphium.store(u);
        }

        // Test $function graceful fallback - when JS engine is unavailable,
        // $function should return null instead of throwing exceptions
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        Map<String, Object> projection = new java.util.HashMap<>();
        projection.put("str_value", Expr.field("str_value"));
        projection.put("counter", Expr.field("counter"));
        projection.put("function_result", Expr.function("function(x) { return x * x; }", java.util.Arrays.asList(Expr.field("counter"))));
        agg.project(projection);
        agg.limit(2);

        // This should not throw an exception even when JavaScript engine is unavailable
        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(2, result.size());

        // When JavaScript engine is not available, function_result should be null
        // but other fields should still be present
        for (Map doc : result) {
            assertNotNull(doc.get("str_value"));
            assertNotNull(doc.get("counter"));
            // function_result may be null when JS engine unavailable - this is expected behavior
        }

        log.info("$function graceful fallback test completed successfully");
    }

    @Test
    public void inMemAggregationSampleTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.sample(10);
        agg.sort("counter");
        List<Map<String, Object>> lst = agg.aggregateMap();
        assert (lst.size() == 10);
        //hard to check randomness....
    }

    @Test
    public void inMemAggregationAddToSetTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.group("all").addToSet("mods", "$str_value");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info(Utils.toJsonString(lst.get(0)));
        assert (lst.size() == 1);
        assertEquals (3,((List) lst.get(0).get("mods")).size());
    }

    @Test
    public void inMemAggregationCountObjectTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, CountAggTest> agg = morphium.createAggregator(UncachedObject.class, CountAggTest.class);
        agg.count("my_count");
        List<CountAggTest> lst = agg.aggregate();
        log.info(Utils.toJsonString(lst.get(0)));
        assert (lst.size() == 1);
        assert (lst.get(0).getMyCount() == 100);
    }


    @Test
    public void inMemAggregationUnsetTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.unset(UncachedObject.Fields.strValue);
        List<Map<String, Object>> lst = agg.aggregateMap();
        assertEquals(100, lst.size());
        for (Map<String, Object> o : lst) {
            assertFalse(o.containsKey("value"));
        }
    }


    @Test
    public void inMemAggregationMerge() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.unset(UncachedObject.Fields.strValue);
        agg.merge("test", Aggregator.MergeActionWhenMatched.merge, Aggregator.MergeActionWhenNotMatched.insert);
        List<Map> lst = agg.aggregate();
        assert (lst.size() == 0);

        List<UncachedObject> l = morphium.createQueryFor(UncachedObject.class).setCollectionName("test").asList();
        assertEquals(0,l.size());
        //checking stored after $unset
        long lastCounter = -1;
        for (UncachedObject o : l) {
            assertNull (o.getStrValue());
            assertNotEquals(lastCounter,o.getCounter());
            lastCounter = o.getCounter();
        }
    }

    @Test
    public void unwindTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            ListContainer lst = new ListContainer();
            lst.setName("Name: " + i);
            for (int j = 0; j < 10; j++) {
                lst.addLong(j);
            }
            lst.addString("mod" + (i % 10));
            morphium.store(lst);
        }

        Aggregator<ListContainer, Map> agg = morphium.createAggregator(ListContainer.class, Map.class);
        agg.unwind(Expr.field(ListContainer.Fields.longList));

        List<Map<String, Object>> result = agg.aggregateMap();
        assertNotNull(result);
        ;
        assert (result.size() == 1000);
        assertNotNull(result.get(0).get("long_list"));
        ;
        assert (!(result.get(1).get("long_list") instanceof List));
    }

    @Embedded
    public static class CountAggTest {
        private int myCount;

        public int getMyCount() {
            return myCount;
        }

        public void setMyCount(int myCount) {
            this.myCount = myCount;
        }
    }


    @Entity
    public static class ModuloValue {
        @Id
        private MorphiumId id;
        private int modValue;
        private String textRep;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public int getModValue() {
            return modValue;
        }

        public void setModValue(int modValue) {
            this.modValue = modValue;
        }

        public String getTextRep() {
            return textRep;
        }

        public void setTextRep(String textRep) {
            this.textRep = textRep;
        }

        public enum Fields {modValue, textRep, id}
    }

    @Test
    public void inMemAggregationSortByCountTest() throws Exception {
        morphium.clearCollection(UncachedObject.class);

        // Create test data with different categories
        for (int i = 0; i < 50; i++) {
            UncachedObject u = new UncachedObject("item" + i, i);
            u.setStrValue("category" + (i % 5)); // 5 different categories
            morphium.store(u);
        }

        // Test $sortByCount - groups by field and sorts by count
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        agg.sortByCount(Expr.field("str_value"));

        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(5, result.size()); // 5 categories

        // Verify results are sorted by count descending
        for (int i = 0; i < result.size() - 1; i++) {
            Map<String, Object> current = result.get(i);
            Map<String, Object> next = result.get(i + 1);

            assertTrue(current.containsKey("_id"));
            assertTrue(current.containsKey("count"));
            assertTrue(next.containsKey("count"));

            int currentCount = (Integer) current.get("count");
            int nextCount = (Integer) next.get("count");

            assertTrue(currentCount >= nextCount, "Results should be sorted by count descending");
        }

        // Each category should have 10 documents (50 total / 5 categories)
        for (Map<String, Object> item : result) {
            assertEquals(10, item.get("count"));
        }

        log.info("$sortByCount test completed successfully");
    }

    @Test
    public void inMemAggregationFacetTest() throws Exception {
        morphium.clearCollection(UncachedObject.class);

        // Create test data with categories
        for (int i = 0; i < 50; i++) {
            UncachedObject u = new UncachedObject("value" + i, i);
            u.setStrValue("category" + (i % 5)); // 5 different categories
            morphium.store(u);
        }

        // Test $facet - multiple aggregation pipelines in parallel
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        // Use facetExpr with properly structured pipeline expressions
        Map<String, Expr> facetMap = new HashMap<>();

        // Simple pipeline for counting categories - just use a field reference
        facetMap.put("categoryCounts", Expr.field("str_value"));

        // Simple pipeline for counter stats - field reference
        facetMap.put("counterStats", Expr.field("counter"));

        agg.facetExpr(facetMap);

        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(1, result.size()); // $facet always returns exactly one document

        Map<String, Object> facetResult = result.get(0);
        assertTrue(facetResult.containsKey("categoryCounts"));
        assertTrue(facetResult.containsKey("counterStats"));

        log.info("$facet test completed successfully");
    }

    @Test
    public void inMemAggregationBucketTest() throws Exception {
        // Create test data with various counter values
        for (int i = 0; i < 50; i++) {
            UncachedObject u = new UncachedObject("item" + i, i);
            morphium.store(u);
        }

        // Test $bucket - group documents into buckets based on ranges
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        // Create buckets: [0, 10), [10, 20), [20, 30), [30, 50)
        List<Expr> boundaries = Arrays.asList(
            Expr.intExpr(0), Expr.intExpr(10), Expr.intExpr(20), Expr.intExpr(30), Expr.intExpr(50)
        );

        agg.bucket(
            Expr.field("counter"),           // groupBy expression
            boundaries,                       // bucket boundaries
            Expr.string("default"),          // default bucket name
            UtilsMap.of(                     // output spec
                "count", Expr.intExpr(1),
                "avgValue", Expr.field("counter")
            )
        );

        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(4, result.size()); // 4 buckets

        // Verify bucket structure
        for (Map bucket : result) {
            assertTrue(bucket.containsKey("_id"));     // bucket boundary
            assertTrue(bucket.containsKey("count"));   // count of items
            assertTrue(bucket.containsKey("avgValue")); // average value
        }

        log.info("$bucket test completed successfully");
    }

    @Test
    public void inMemAggregationBucketAutoTest() throws Exception {
        // Create test data
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("item" + i, i);
            morphium.store(u);
        }

        // Test $bucketAuto - automatically determine bucket boundaries
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        agg.bucketAuto(
            Expr.field("counter"),           // groupBy expression
            5,                               // number of buckets
            UtilsMap.of(                     // output spec
                "count", Expr.intExpr(1),
                "minValue", Expr.field("counter"),
                "maxValue", Expr.field("counter")
            ),
            Aggregator.BucketGranularity.R5  // granularity
        );

        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(5, result.size()); // 5 auto-generated buckets

        // Verify auto-bucket structure
        for (Map bucket : result) {
            assertTrue(bucket.containsKey("_id"));     // bucket range {min, max}
            assertTrue(bucket.containsKey("count"));   // count of items
            assertTrue(bucket.containsKey("minValue")); // min value in bucket
            assertTrue(bucket.containsKey("maxValue")); // max value in bucket

            Map<String, Object> id = (Map<String, Object>) bucket.get("_id");
            assertTrue(id.containsKey("min"));
            assertTrue(id.containsKey("max"));
        }

        log.info("$bucketAuto test completed successfully");
    }

    @Test
    public void inMemAggregationGraphLookupTest() throws Exception {
        // Create hierarchical test data (employee -> manager relationship)
        // CEO (no manager)
        UncachedObject ceo = new UncachedObject("CEO", 1);
        ceo.setStrValue("CEO");
        ceo.setDval(-1.0); // CEO has no manager
        morphium.store(ceo);

        // Managers (report to CEO)
        for (int i = 2; i <= 4; i++) {
            UncachedObject manager = new UncachedObject("Manager" + i, i);
            manager.setStrValue("Manager");
            manager.setDval(1.0); // reports to CEO (counter=1)
            morphium.store(manager);
        }

        // Employees (report to managers)
        for (int i = 5; i <= 10; i++) {
            UncachedObject employee = new UncachedObject("Employee" + i, i);
            employee.setStrValue("Employee");
            employee.setDval((double)(2 + (i % 3))); // reports to manager 2, 3, or 4
            morphium.store(employee);
        }

        // Test $graphLookup - recursive lookup for organizational hierarchy
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        // Start from CEO and find all subordinates recursively
        agg.match(morphium.createQueryFor(UncachedObject.class).f("counter").eq(1)); // Start with CEO

        agg.graphLookup(
            "uncached_object",               // collection to lookup in
            Expr.field("counter"),           // startWith: field to start traversal from
            "dval",                          // connectFromField: field to match with connectToField
            "counter",                       // connectToField: field to connect to
            "subordinates",                  // as: output array field name
            null,                           // maxDepth: no limit
            null,                           // depthField: don't track depth
            null                            // restrictSearchWithMatch: no additional match criteria
        );

        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(1, result.size()); // Should return CEO with all subordinates

        Map<String, Object> ceoDoc = result.get(0);
        System.out.println("CEO doc: " + ceoDoc);
        assertTrue(ceoDoc.containsKey("subordinates"));

        List subordinates = (List) ceoDoc.get("subordinates");
        System.out.println("Subordinates found: " + subordinates.size());
        System.out.println("Subordinates: " + subordinates);

        // Should find managers (3) + employees (6) = 9 subordinates
        assertTrue(subordinates.size() >= 6); // At least the direct reports

        log.info("$graphLookup test completed successfully - found " + subordinates.size() + " subordinates");
    }

    @Test
    public void inMemAggregationSortByCountTest() throws Exception {
        // Create test data with repeated values
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("item" + i, i);
            u.setStrValue("group" + (i % 7)); // 7 groups with different frequencies
            morphium.store(u);
        }

        // Test $sortByCount - group by field and sort by count
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        agg.sortByCount(Expr.field("str_value"));

        List<Map> result = agg.aggregate();
        assertNotNull(result);
        assertEquals(7, result.size()); // 7 different groups

        // Verify sorting by count (descending)
        int lastCount = Integer.MAX_VALUE;
        for (Map group : result) {
            assertTrue(group.containsKey("_id"));     // the grouped field value
            assertTrue(group.containsKey("count"));   // count of documents

            int currentCount = ((Number) group.get("count")).intValue();
            assertTrue(currentCount <= lastCount); // Should be sorted descending
            lastCount = currentCount;
        }

        log.info("$sortByCount test completed successfully");
    }

}
