package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 10:59
 * <p/>
 */
public class AggregationTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aggregatorIdTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Aggregator<UncachedObject, Map> a = morphium.createAggregator(UncachedObject.class, Map.class);
            a.group(Doc.of("cnt","$counter","str","$str_value")).sum("sum", "$counter").end();
            // a.group("all").sum("sum", "$counter");
            var res=a.aggregateMap();
            log.info("Size: "+res.size());
            assertTrue(res.get(0).get("_id") instanceof Map);
            assertFalse(((Map)res.get(0).get("_id")).get("cnt") instanceof String);

        }
    }
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aggregatorTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 1000);
            Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
            assertNotNull(a.getResultType());;
            //eingangsdaten reduzieren
            a = a.project("counter");
            //Filtern
            a = a.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(100));
            //Sortieren - für $first/$last
            a = a.sort("counter");
            //limit der Daten
            a = a.limit(15);
            //group by - in dem Fall ALL, könnte auch beliebig sein
            a = a.group("all").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();
            //        a = a.group("a2").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();
            //ergebnis projezieren
            HashMap<String, Object> projection = new HashMap<>();
            projection.put("summe", 1);
            projection.put("anzahl", "$anz");
            projection.put("schnitt", 1);
            projection.put("last", "$letzter");
            projection.put("first", "$erster");
            a = a.project(projection);
            List<Aggregate> lst = a.aggregate();
            assert(lst.size() == 1) : "Size wrong: " + lst.size();
            log.debug("Sum  : " + lst.get(0).getSumme());
            log.debug("Avg  : " + lst.get(0).getSchnitt());
            log.debug("Last :    " + lst.get(0).getLast());
            log.debug("First:   " + lst.get(0).getFirst());
            log.debug("count:  " + lst.get(0).getAnzahl());
            assert(lst.get(0).getAnzahl() == 15) : "did not find 15, instead found: " + lst.get(0).getAnzahl();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aggregatorExprTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("Running test with " + morphium.getDriver().getName());
            createUncachedObjects(morphium, 1000);
            Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
            assertNotNull(a.getResultType());;
            //eingangsdaten reduzieren
            a = a.project(UtilsMap.of("counter", (Object) Expr.intExpr(1), "cnt2", Expr.field("counter")));
            //Filtern
            a = a.match(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.intExpr(100)));
            //Sortieren - für $first/$last
            a = a.sort("counter");
            //limit der Daten
            a = a.limit(15);
            //group by - in dem Fall alle, könnte auch beliebig sein
            a = a.group(Expr.nullExpr()).expr("schnitt", Expr.avg(Expr.field(UncachedObject.Fields.counter))).expr("summe", Expr.sum(Expr.field(UncachedObject.Fields.counter)))
             .expr("anz", Expr.sum(Expr.intExpr(1))).expr("letzter", Expr.last(Expr.field("counter"))).expr("erster", Expr.first(Expr.field("counter"))).end();
            //ergebnis projezieren
            HashMap<String, Object> projection = new HashMap<>();
            projection.put("summe", 1);
            projection.put("anzahl", "$anz");
            projection.put("schnitt", 1);
            projection.put("last", "$letzter");
            projection.put("first", "$erster");
            a = a.project(projection);
            List<Aggregate> lst = a.aggregate();
            assertEquals(1, lst.size());
            log.debug("Sum  : " + lst.get(0).getSumme());
            log.debug("Avg  : " + lst.get(0).getSchnitt());
            log.debug("Last :    " + lst.get(0).getLast());
            log.debug("First:   " + lst.get(0).getFirst());
            log.debug("count:  " + lst.get(0).getAnzahl());
            assert(lst.get(0).getAnzahl() == 15) : "did not find 15, instead found: " + lst.get(0).getAnzahl();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testPush(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.clearCollection(UncachedObject.class);

            for (int i = 0; i < 100; i++) {
                UncachedObject u = new UncachedObject();
                u.setCounter(i % 3);
                u.setStrValue("" + i % 5);
                morphium.store(u);
            }

            Aggregator<UncachedObject, AggregatePush> agr = morphium.createAggregator(UncachedObject.class, AggregatePush.class);
            agr.group("$str_value").sum("count", 1).sum("sum_counts", "$counter").push("values", "counter", "$counter").end().sort("sum_counts");
            List<AggregatePush> lst = agr.aggregate();
            assertNotNull(lst);;
            assertEquals(5,lst.size());
            assertEquals(20,lst.get(0).getCount());
            assertEquals(19,lst.get(0).getSumCounts());
            assertEquals(20,lst.get(0).getValues().size() );
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testAddToSet(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.clearCollection(UncachedObject.class);

            //noinspection Duplicates
            for (int i = 0; i < 100; i++) {
                UncachedObject u = new UncachedObject();
                u.setCounter(i % 3);
                u.setStrValue("" + i % 5);
                morphium.store(u);
            }

            Aggregator<UncachedObject, AggregatePush> agr = morphium.createAggregator(UncachedObject.class, AggregatePush.class);
            //Ending a group is not longer necessary... but the aggregator will warn!
            agr.group("$str_value").sum("count", 1).sum("sum_counts", "$counter").addToSet("values", "counter", "$counter");
            List<AggregatePush> lst = agr.aggregate();
            assertNotNull(lst);;
            assertEquals(5, lst.size());
            assertEquals(3, lst.get(0).getValues().size());
            assertTrue(lst.get(0).getSumCounts() >= 19);
            assertTrue(lst.get(0).getCount() == 20);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aggregationTestcompare(Morphium morphium) throws Exception {
        try (morphium) {
            log.debug("Preparing data");
            createUncachedObjects(morphium, 10000);
            log.debug("done... starting");
            long start = System.currentTimeMillis();
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            HashMap<Integer, Integer> sum = new HashMap<>();
            HashMap<Integer, Integer> anz = new HashMap<>();
            q = q.sort("counter");

            for (UncachedObject u : q.asList()) {
                int v = u.getCounter() % 3;

                if (sum.get(v) == null) {
                    sum.put(v, u.getCounter());
                } else {
                    sum.put(v, sum.get(v) + v);
                }

                anz.merge(v, 1, (a, b)->a + b);
            }

            for (Integer v : sum.keySet()) {
                log.debug("ID: " + v);
                log.debug("  anz: " + anz.get(v));
                log.debug("  sum: " + sum.get(v));
                log.debug("  avg: " + (sum.get(v) / anz.get(v)));
            }

            long dur = System.currentTimeMillis() - start;
            log.debug("Query took " + dur + "ms");
            log.debug("Starting test with Aggregation:");
            start = System.currentTimeMillis();
            Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
            assertNotNull(a.getResultType());;
            ArrayList<Object> params = new ArrayList<>();
            params.add("$counter");
            params.add(3);
            Map<String, Object> db = new HashMap<>();
            db.put("$mod", params);
            a = a.sort("$counter");
            a = a.group(db).sum("summe", "$counter").sum("anzahl", 1).avg("schnitt", "$counter").end();
            List<Aggregate> lst = a.aggregate();
            assertEquals(3, lst.size());

            for (Aggregate ag : lst) {
                log.debug("ID: " + ag.getTheGeneratedId());
                log.debug(" sum:" + ag.getSumme());
                log.debug(" anz:" + ag.getAnzahl());
                log.debug(" avg:" + ag.getSchnitt());
            }

            dur = System.currentTimeMillis() - start;
            log.debug("Aggregation took " + dur + "ms");
        }
    }

    @Embedded
    public static class Aggregate {
        private double schnitt;
        private long summe;
        private int last;
        private int first;
        private int anzahl;

        @Property(fieldName = "_id")
        private String theGeneratedId;

        public int getAnzahl() {
            return anzahl;
        }

        public void setAnzahl(int anzahl) {
            this.anzahl = anzahl;
        }

        public int getLast() {
            return last;
        }

        public void setLast(int last) {
            this.last = last;
        }

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public double getSchnitt() {
            return schnitt;
        }

        public void setSchnitt(double schnitt) {
            this.schnitt = schnitt;
        }

        public long getSumme() {
            return summe;
        }

        public void setSumme(long summe) {
            this.summe = summe;
        }

        public String getTheGeneratedId() {
            return theGeneratedId;
        }

        public void setTheGeneratedId(String theGeneratedId) {
            this.theGeneratedId = theGeneratedId;
        }
    }

    @Embedded
    public static class AggregatePush {
        private List<Map<String, Object>> values;
        private long sumCounts;
        private long count;

        @Property(fieldName = "_id")
        private String theGeneratedId;

        public List<Map<String, Object>> getValues() {
            return values;
        }

        public long getSumCounts() {
            return sumCounts;
        }

        public long getCount() {
            return count;
        }

        public String getTheGeneratedId() {
            return theGeneratedId;
        }

    }

}
