package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 10:59
 * <p/>
 */
public class Aggregation extends MorphiumTestBase {
    @Test
    public void aggregatorTest() {
        createUncachedObjects(1000);

        Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
        assert (a.getResultType() != null);
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
        assert (lst.size() == 1) : "Size wrong: " + lst.size();
        log.info("Sum  : " + lst.get(0).getSumme());
        log.info("Avg  : " + lst.get(0).getSchnitt());
        log.info("Last :    " + lst.get(0).getLast());
        log.info("First:   " + lst.get(0).getFirst());
        log.info("count:  " + lst.get(0).getAnzahl());


        assert (lst.get(0).getAnzahl() == 15) : "did not find 15, instead found: " + lst.get(0).getAnzahl();

    }

    @Test
    public void aggregatorExprTest() {
        createUncachedObjects(1000);

        Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
        assert (a.getResultType() != null);
        //eingangsdaten reduzieren
        a = a.project(UtilsMap.of("counter", (Object) Expr.intExpr(1), "cnt2", Expr.field("counter")));
        //Filtern
//        a = a.match(morphium.createQueryFor(UncachedObject.class).f("counter").gt(100));
        a = a.match(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.intExpr(100)));
        //Sortieren - für $first/$last
        a = a.sort("counter");
        //limit der Daten
        a = a.limit(15);
        //group by - in dem Fall alle, könnte auch beliebig sein
        a = a.group(Expr.string(null)).expr("schnitt", Expr.avg(Expr.field(UncachedObject.Fields.counter))).expr("summe", Expr.sum(Expr.field(UncachedObject.Fields.counter))).expr("anz", Expr.sum(Expr.intExpr(1))).expr("letzter", Expr.last(Expr.field("counter"))).expr("erster", Expr.first(Expr.field("counter"))).end();
        //a = a.group("null").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();
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
        assert (lst.size() == 1) : "Size wrong: " + lst.size();
        log.info("Sum  : " + lst.get(0).getSumme());
        log.info("Avg  : " + lst.get(0).getSchnitt());
        log.info("Last :    " + lst.get(0).getLast());
        log.info("First:   " + lst.get(0).getFirst());
        log.info("count:  " + lst.get(0).getAnzahl());


        assert (lst.get(0).getAnzahl() == 15) : "did not find 15, instead found: " + lst.get(0).getAnzahl();

    }

    @Test
    public void testPush() {
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
        assert (lst != null);
        assert (lst.size() == 5);
        assert (lst.get(0).getCount() == 20);
        assert (lst.get(0).getSumCounts() == 19);
        assert (lst.get(0).getValues().size() == 20);
    }

    @Test
    public void testAddToSet() {
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
        agr.group("$str_value").sum("count", 1).sum("sum_counts", "$counter").addToSet("values", "$counter");
        List<AggregatePush> lst = agr.aggregate();
        assert (lst != null);
        assert (lst.size() == 5);
        assert (lst.get(0).getValues().size() == 3);
        assert (lst.get(0).getSumCounts() >= 19);
        assert (lst.get(0).getCount() == 20);

    }

    @Test
    public void aggregationTestcompare() {
        log.info("Preparing data");
        createUncachedObjects(10000);
        log.info("done... starting");
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
            anz.merge(v, 1, (a, b) -> a + b);

        }
        for (Integer v : sum.keySet()) {
            log.info("ID: " + v);
            log.info("  anz: " + anz.get(v));
            log.info("  sum: " + sum.get(v));
            log.info("  avg: " + (sum.get(v) / anz.get(v)));
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Query took " + dur + "ms");

        log.info("Starting test with Aggregation:");
        start = System.currentTimeMillis();
        Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
        assert (a.getResultType() != null);
        ArrayList<Object> params = new ArrayList<>();
        params.add("$counter");
        params.add(3);
        Map<String, Object> db = new HashMap<>();
        db.put("$mod", params);
        a = a.sort("$counter");
        a = a.group(db).sum("summe", "$counter").sum("anzahl", 1).avg("schnitt", "$counter").end();
        List<Aggregate> lst = a.aggregate();
        assert (lst.size() == 3);
        for (Aggregate ag : lst) {
            log.info("ID: " + ag.getTheGeneratedId());
            log.info(" sum:" + ag.getSumme());
            log.info(" anz:" + ag.getAnzahl());
            log.info(" avg:" + ag.getSchnitt());
        }
        dur = System.currentTimeMillis() - start;
        log.info("Aggregation took " + dur + "ms");
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
