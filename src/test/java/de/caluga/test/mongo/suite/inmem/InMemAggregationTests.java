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

}
