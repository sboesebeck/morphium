package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * #381: the aggregation pipeline compared every Number via {@code doubleValue()}, so adjacent
 * longs beyond 2^53 collapsed onto each other - in {@code $bucket} boundary tests, in the
 * pipeline {@code $sort}, and in the {@code $sortArray}/{@code $maxN}/{@code $minN} expression
 * operators - although #379 had made {@code $match} on the same values exact. Snowflake ids
 * (~1.8e18) and nanosecond timestamps live in that range; a wrong bucket or a wrong order
 * there is silent.
 */
@Tag("inmemory")
public class AggregatorLongPrecisionTest {

    private static final long TWO_POW_53 = 1L << 53;   // 9007199254740992: the last exact double

    private Morphium morphium;
    private InMemoryDriver drv;
    private final String db = "agg_long_precision";
    private final String coll = "agg_longs";

    @Entity(collectionName = "agg_longs")
    public static class LongDoc {
        @Id
        public MorphiumId id;
        public long v;
    }

    @BeforeEach
    public void setup() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig(db, 10, 10000, 1000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        drv = (InMemoryDriver) morphium.getDriver();
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    private void insert(long... values) throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long v : values) {
            docs.add(Doc.of("_id", new MorphiumId(), "v", v));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
    }

    @SuppressWarnings("unchecked")
    private Aggregator<LongDoc, Map> aggregator() {
        Aggregator<LongDoc, Map> agg = (Aggregator<LongDoc, Map>) drv.createAggregator(morphium, LongDoc.class, Map.class);
        agg.setCollectionName(coll);
        return agg;
    }

    private static List<Long> longs(List<Map<String, Object>> docs, String field) {
        List<Long> out = new ArrayList<>();
        for (Map<String, Object> d : docs) {
            out.add(((Number) d.get(field)).longValue());
        }
        return out;
    }

    @Test
    public void bucketBoundariesPast2Pow53AreExact() throws Exception {
        // 2^53 and 2^53+1 are the same double; with double comparison 2^53 satisfied
        // "< 2^53+1" as false and fell into the [2^53+1, 2^53+2) bucket.
        insert(TWO_POW_53, TWO_POW_53 + 1);
        Aggregator<LongDoc, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$bucket", Doc.of(
            "groupBy", "$v",
            "boundaries", List.of(TWO_POW_53, TWO_POW_53 + 1, TWO_POW_53 + 2),
            "default", "other")));
        agg.addOperator(UtilsMap.of("$sort", Doc.of("_id", 1)));

        List<Map<String, Object>> res = agg.aggregateMap();

        assertEquals(2, res.size(), "one bucket per value: " + res);
        assertEquals(TWO_POW_53, ((Number) res.get(0).get("_id")).longValue());
        assertEquals(1, ((Number) res.get(0).get("count")).intValue(), "2^53 belongs to [2^53, 2^53+1): " + res);
        assertEquals(TWO_POW_53 + 1, ((Number) res.get(1).get("_id")).longValue());
        assertEquals(1, ((Number) res.get(1).get("count")).intValue(), "2^53+1 belongs to [2^53+1, 2^53+2): " + res);
    }

    @Test
    public void pipelineSortOrdersLongsPast2Pow53() throws Exception {
        // inserted out of order; a stable sort that sees 2^53+1 == 2^53 keeps the insertion order
        insert(TWO_POW_53 + 1, TWO_POW_53, TWO_POW_53 + 3, TWO_POW_53 + 2);
        Aggregator<LongDoc, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("v", 1)));

        List<Map<String, Object>> res = agg.aggregateMap();

        assertEquals(List.of(TWO_POW_53, TWO_POW_53 + 1, TWO_POW_53 + 2, TWO_POW_53 + 3), longs(res, "v"));
    }

    @Test
    public void pipelineSortDescendingOrdersLongsPast2Pow53() throws Exception {
        insert(TWO_POW_53, TWO_POW_53 + 1);
        Aggregator<LongDoc, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("v", -1)));

        List<Map<String, Object>> res = agg.aggregateMap();

        assertEquals(List.of(TWO_POW_53 + 1, TWO_POW_53), longs(res, "v"));
    }

    @Test
    public void matchThenSortAgreeOnLongsPast2Pow53() throws Exception {
        // $match is exact since #379; the $sort behind it must not disagree with it
        insert(TWO_POW_53 + 2, TWO_POW_53, TWO_POW_53 + 1);
        Aggregator<LongDoc, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$match", Doc.of("v", Doc.of("$gte", TWO_POW_53 + 1))));
        agg.addOperator(UtilsMap.of("$sort", Doc.of("v", 1)));

        List<Map<String, Object>> res = agg.aggregateMap();

        assertEquals(List.of(TWO_POW_53 + 1, TWO_POW_53 + 2), longs(res, "v"));
    }

    @Test
    public void groupMaxAndMinPickTheRightLongPast2Pow53() throws Exception {
        insert(TWO_POW_53, TWO_POW_53 + 1);
        Aggregator<LongDoc, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$group", Doc.of("_id", null,
            "mx", Doc.of("$max", "$v"),
            "mn", Doc.of("$min", "$v"))));

        List<Map<String, Object>> res = agg.aggregateMap();

        assertEquals(1, res.size());
        assertEquals(TWO_POW_53 + 1, ((Number) res.get(0).get("mx")).longValue(), "$max: " + res);
        assertEquals(TWO_POW_53, ((Number) res.get(0).get("mn")).longValue(), "$min: " + res);
    }

    @Test
    public void windowRankSeparatesAdjacentLongsPast2Pow53() throws Exception {
        // $setWindowFields sorts and ranks with the aggregator's own comparator; a rank that
        // sees 2^53+1 == 2^53 gives both documents rank 1
        insert(TWO_POW_53 + 1, TWO_POW_53);
        Aggregator<LongDoc, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$setWindowFields", Doc.of(
            "sortBy", Doc.of("v", 1),
            "output", Doc.of("r", Doc.of("$rank", Doc.of())))));
        agg.addOperator(UtilsMap.of("$sort", Doc.of("v", 1)));

        List<Map<String, Object>> res = agg.aggregateMap();

        assertEquals(List.of(TWO_POW_53, TWO_POW_53 + 1), longs(res, "v"));
        assertEquals(List.of(1L, 2L), longs(res, "r"), "ranks: " + res);
    }

    @Test
    public void sortArrayOrdersLongsPast2Pow53() {
        Map<String, Object> ctx = Doc.of("arr", List.of(TWO_POW_53 + 1, TWO_POW_53, TWO_POW_53 + 2));

        Object asc = Expr.sortArray(Expr.field("arr"), Expr.intExpr(1)).evaluate(ctx);
        Object desc = Expr.sortArray(Expr.field("arr"), Expr.intExpr(-1)).evaluate(ctx);

        assertEquals(List.of(TWO_POW_53, TWO_POW_53 + 1, TWO_POW_53 + 2), asc);
        assertEquals(List.of(TWO_POW_53 + 2, TWO_POW_53 + 1, TWO_POW_53), desc);
    }

    @Test
    public void maxNAndMinNPickTheRightLongPast2Pow53() {
        Map<String, Object> ctx = Doc.of("arr", List.of(TWO_POW_53, TWO_POW_53 + 1));

        assertEquals(List.of(TWO_POW_53 + 1), Expr.maxN(Expr.field("arr"), Expr.intExpr(1)).evaluate(ctx));
        assertEquals(List.of(TWO_POW_53), Expr.minN(Expr.field("arr"), Expr.intExpr(1)).evaluate(ctx));
    }

    @Test
    public void setOperatorsDoNotMergeAdjacentLongsPast2Pow53() {
        // $setUnion de-duplicates with numeric equality; 2^53 and 2^53+1 are two elements
        Map<String, Object> ctx = Doc.of("a", List.of(TWO_POW_53), "b", List.of(TWO_POW_53 + 1));

        Object union = Expr.parse(Doc.of("$setUnion", List.of("$a", "$b"))).evaluate(ctx);

        assertEquals(2, ((List<?>) union).size(), "2^53 and 2^53+1 are distinct set elements: " + union);
    }
}
