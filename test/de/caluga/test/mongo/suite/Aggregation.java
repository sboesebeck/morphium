package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 10:59
 * <p/>
 * TODO: Add documentation here
 */
public class Aggregation extends MongoTest {
    @Test
    public void aggregatorTest() throws Exception {
        createUncachedObjects(1000);

        Aggregator<UncachedObject, Aggregate> a = MorphiumSingleton.get().createAggregator(UncachedObject.class, Aggregate.class);
        assert (a.getResultType() != null);
        //eingangsdaten reduzieren
        a = a.project("counter");
        //Filtern
        a = a.match(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").gt(100));
        //Sortieren - für $first/$last
        a = a.sort("counter");
        //limit der Daten
        a = a.limit(15);
        //group by - in dem Fall ALL, könnte auch beliebig sein
        a = a.group("all").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();
        //ergebnis projezieren
        a = a.project(new BasicDBObject("summe", 1).append("anzahl", "$anz").append("schnitt", 1).append("last", "$letzter").append("first", "$erster"));

        List<DBObject> obj = a.toAggregationList();
        for (DBObject o : obj) {
            log.info("Object: " + o.toString());
        }
        List<Aggregate> lst = a.aggregate();
        assert (lst.size() == 1);
        log.info("Summe:   " + lst.get(0).getSumme());
        log.info("Schnitt: " + lst.get(0).getSchnitt());
        log.info("Last:    " + lst.get(0).getLast());
        log.info("First:   " + lst.get(0).getFirst());
        log.info("Anzahl:  " + lst.get(0).getAnzahl());
        assert (lst.get(0).getAnzahl() == 15);

    }

    @Test
    public void aggregationTestcompare() throws Exception {
        log.info("Preparing data");
        createUncachedObjects(100000);
        log.info("done... starting");
        long start = System.currentTimeMillis();
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        HashMap<Integer, Integer> sum = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> anz = new HashMap<Integer, Integer>();
        q = q.sort("counter");

        for (UncachedObject u : q.asList()) {
            int v = u.getCounter() % 3;
            if (sum.get(v) == null) {
                sum.put(v, u.getCounter());
            } else {
                sum.put(v, sum.get(v).intValue() + v);
            }
            if (anz.get(v) == null) {
                anz.put(v, 1);
            } else {
                anz.put(v, anz.get(v).intValue() + 1);
            }

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
        Aggregator<UncachedObject, Aggregate> a = MorphiumSingleton.get().createAggregator(UncachedObject.class, Aggregate.class);
        assert (a.getResultType() != null);
        BasicDBList params = new BasicDBList();
        params.add("$counter");
        params.add(3);
        BasicDBObject db = new BasicDBObject("$mod", params);
        a = a.sort("$counter");
        a = a.group(db).sum("summe", "$counter").sum("anzahl", 1).avg("schnitt", "$counter").end();
        List<DBObject> obj = a.toAggregationList();
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


}
