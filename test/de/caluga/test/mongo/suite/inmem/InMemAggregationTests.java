package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class InMemAggregationTests extends MorphiumInMemTestBase {

    @Test
    public void inMemAggregationSumTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.value).eq("mod0"));
        agg.group("$value").sum("summe", "$counter").sum("cnt", 1).end();
        agg.addFields(Utils.getMap("tst", Expr.field("summe")));
        agg.project("avg", Expr.divide(Expr.field("tst"), Expr.field("cnt")));
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info("Count: " + lst.size());
        for (Map<String, Object> o : lst) {
            log.info(Utils.toJsonString(o));
        }

        assert (lst.size() == 1);
        assert (((Number) lst.get(0).get("summe")).doubleValue() == 1683);
        assert (((Number) lst.get(0).get("tst")).doubleValue() == 1683);
        assert (((Number) lst.get(0).get("avg")).doubleValue() == 49.5);
        assert (((Number) lst.get(0).get("cnt")).doubleValue() == 34);
        assert (lst.get(0).get("_id").equals("mod0"));
    }


    @Test
    public void inMemAggregationFirstLastTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.group("$value").first("cnt", "$counter").last("lst", "$counter").end();
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
        agg.sort("value", "-counter");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info("Count: " + lst.size());
        String lastValue = "mod0";
        int lastCounter = 100;
        for (Map<String, Object> o : lst) {
            log.info(Utils.toJsonString(o));
            if (lastValue.equals(o.get("valuie"))) {
                assert (((Number) o.get("counter")).intValue() < lastCounter) : "LastCounter: " + lastCounter + " got: " + o.get("counter");
                lastCounter = ((Number) o.get("counter")).intValue();
            }
            assert (lastValue.compareTo((String) o.get("value")) <= 0) : "LastValue: " + lastValue + " current: " + o.get("value");
            lastValue = (String) o.get("value");
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
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod", i % 3);
            morphium.store(u);
        }
        for (int i = 0; i < 5; i++) {
            ModuloValue mv = new ModuloValue();
            mv.setModValue(i);
            mv.setTextRep("Modulo is " + i);
            morphium.store(mv);
        }

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.lookup(ModuloValue.class, UncachedObject.Fields.counter, ModuloValue.Fields.modValue, "modValue", null, null);
        agg.unwind("modValue");
        agg.group(Expr.field("_id")).first("modValue", Expr.field("mod_value"));
        List<Map> ret = agg.aggregate();

        assert (ret != null);
        for (Map m : ret) {
            assert (m.get("modValue") != null);
            assert (((Map) m.get("modValue")).get("mod_value").equals(m.get("counter")));
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
        agg.group("all").addToSet("mods", "$value");
        List<Map<String, Object>> lst = agg.aggregateMap();
        log.info(Utils.toJsonString(lst.get(0)));
        assert (lst.size() == 1);
        assert (((List) lst.get(0).get("mods")).size() == 3);
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
        agg.unset(UncachedObject.Fields.value);
        List<Map<String, Object>> lst = agg.aggregateMap();
        assert (lst.size() == 100);
        for (Map<String, Object> o : lst) {
            assert (!o.containsKey("value"));
        }
    }


    @Test
    public void inMemAggregationMerge() throws Exception {
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject("mod" + (i % 3), i);
            morphium.store(u);
        }
        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.unset(UncachedObject.Fields.value);
        agg.merge("test", Aggregator.MergeActionWhenMatched.merge, Aggregator.MergeActionWhenNotMatched.insert);
        List<Map> lst = agg.aggregate();
        assert (lst.size() == 0);

        List<UncachedObject> l = morphium.createQueryFor(UncachedObject.class).setCollectionName("test").asList();
        assert (l.size() != 0);
        //checking stored after $unset
        long lastCounter = -1;
        for (UncachedObject o : l) {
            assert (o.getValue() == null);
            assert (o.getCounter() != lastCounter);
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
        assert (result != null);
        assert (result.size() == 1000);
        assert (result.get(0).get("long_list") != null);
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
