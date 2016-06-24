package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 22.11.12
 * Time: 08:30
 * <p/>
 */
public class GeoSearchTests extends MongoTest {

    @Test
    public void nearTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<>();
        //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<>();
            pos.add((Math.random() * 180) - 90);
            pos.add((Math.random() * 180) - 90);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").near(0, 0, 10);
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 (10)");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Test
    public void nearSphereTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<>();
        //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<>();
            pos.add((Math.random() * 6) - 3);
            pos.add((Math.random() * 6) - 3);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").nearSphere(0, 0);
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 ");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Test
    public void boxTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<>();
        //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<>();
            pos.add((Math.random() * 180) - 90);
            pos.add((Math.random() * 180) - 90);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").box(0, 0, 10, 10);
        log.info("Query: " + q.toQueryObject().toString());
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 -> (10,10)");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Test
    public void centerSphereTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<>();
        //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<>();
            pos.add((Math.random() * Math.PI) - Math.PI / 2);
            pos.add((Math.random() * Math.PI) - Math.PI / 2);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").centerSphere(0, 0, 0.01);
        log.info("Query: " + q.toQueryObject().toString());
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 -> 0.01 rad");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Test
    public void polygonTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<>();
        //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<>();
            pos.add((Math.random() * 180) - 90);
            pos.add((Math.random() * 180) - 90);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").polygon(0, 0, 0, 10, 10, 80, 80, 0);
        log.info("Query: " + q.toQueryObject().toString());
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 -> (10,10)");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Test
    public void centerTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<>();
        //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<>();
            pos.add((Math.random() * 180) - 90);
            pos.add((Math.random() * 180) - 90);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").center(0, 0, 10);
        log.info("Query: " + q.toQueryObject().toString());
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 -> 10");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Index("position:2d")
    @NoCache
    @WriteBuffer(false)
    @WriteSafety(level = SafetyLevel.MAJORITY)
    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    @Entity
    public static class Place {
        public List<Double> position;
        public String name;
        @Id
        private MorphiumId id;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public List<Double> getPosition() {
            return position;
        }

        public void setPosition(List<Double> position) {
            this.position = position;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
