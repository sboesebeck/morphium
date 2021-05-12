package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
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
}
