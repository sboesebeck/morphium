package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 22.11.12
 * Time: 08:30
 * <p/>
 */
@Tag("core")
public class GeoSearchTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nearTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(Place.class);
            ArrayList<Place> toStore = new ArrayList<>();
            morphium.ensureIndicesFor(Place.class);

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
            List<Place> lst = q.asList();
            log.info("Found " + lst.size() + " places around 0,0 (10)");

            for (Place p : lst) {
                log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nearSphereTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(Place.class);
            ArrayList<Place> toStore = new ArrayList<>();
            morphium.ensureIndicesFor(Place.class);

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
            List<Place> lst = q.asList();
            log.info("Found " + lst.size() + " places around 0,0 ");

            for (Place p : lst) {
                log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void boxTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(Place.class);
            ArrayList<Place> toStore = new ArrayList<>();
            morphium.ensureIndicesFor(Place.class);

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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void centerSphereTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(Place.class);
            ArrayList<Place> toStore = new ArrayList<>();
            morphium.ensureIndicesFor(Place.class);

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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void polygonTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(Place.class);
            ArrayList<Place> toStore = new ArrayList<>();
            morphium.ensureIndicesFor(Place.class);

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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void centerTest(Morphium morphium) {
        try (morphium) {
            morphium.dropCollection(Place.class);
            ArrayList<Place> toStore = new ArrayList<>();
            morphium.ensureIndicesFor(Place.class);

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
    }

    @Index(value = {"position:2d"})
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

        public enum Fields {name, position, id}
    }
}
