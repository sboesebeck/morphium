package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;
import org.junit.Test;

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
        a = a.project("counter");
        a = a.match(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").gt(100));
        a = a.sort("counter");
        a = a.limit(15);
        a = a.group("all").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();
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
