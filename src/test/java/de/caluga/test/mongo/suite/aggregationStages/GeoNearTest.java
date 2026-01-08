package de.caluga.test.mongo.suite.aggregationStages;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.geospatial.Point;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@Tag("aggregation")
@Tag("external")  // Requires MongoDB - $geoNear not supported by InMemoryDriver
public class GeoNearTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testGeoNear(Morphium morphium) throws Exception  {
        morphium.dropCollection(Place.class);
        Thread.sleep(100);
        morphium.ensureIndicesFor(Place.class);
        // Wait for index creation to complete - geospatial indexes can take time
        Thread.sleep(1000);

        morphium.store(new Place("Polo grounds", new Point(-73.9375, 40.8303), new Double[]{-73.9375, 40.8303}, "Stadiums"));
        morphium.store(new Place("Central park", new Point(-73.97, 40.77), new Double[]{-73.97, 40.77}, "Parks"));
        morphium.store(new Place("La Guardia", new Point(-73.88, 40.78), new Double[]{-73.88, 40.78}, "Airport"));
        morphium.store(new Place("Englischer Garten", new Point(11.591944, 48.152779), new Double[]{11.624707, 48.218800}, "Parks"));
        morphium.store(new Place("Allianz Arena", new Point(11.624707, 48.218800), new Double[]{11.624707, 48.218800}, "Stadiums"));
        morphium.store(new Place("Westfalenhalle Dortmund", new Point(7.463520, 51.511030), new Double[]{7.463520, 51.511030}, "Stadiums"));

        // Wait for documents to be indexed
        Thread.sleep(500);

        // Verify we have the expected number of documents
        long count = morphium.createQueryFor(Place.class).countAll();
        assert count == 6 : "Expected 6 places but found " + count;

        Aggregator<Place, Map> agg = morphium.createAggregator(Place.class, Map.class);
        agg.geoNear(UtilsMap.of(Aggregator.GeoNearFields.near, (Object) new Point(-73.98142, 40.71782),
                Aggregator.GeoNearFields.key, "location",
                Aggregator.GeoNearFields.distanceField, "dist.calculated",
                Aggregator.GeoNearFields.query, morphium.createQueryFor(Place.class).f("category").eq("Stadiums").toQueryObject())
        );

        List<Map<String, Object>> result = agg.aggregateMap();
        assert (result.size() == 3);

        for (Map<String, Object> m : result) {
            log.info("Result: " + m.toString());
            assert (m.get("category").equals("Stadiums"));
            assert (m.get("dist") instanceof Map);
            assert (((Map) m.get("dist")).get("calculated") instanceof Double);
        }

    }


    @Entity
    @Index(value = {"location:2dsphere", "legacy:2d"})
    public static class Place {
        @Id
        public MorphiumId id;
        public String name;
        public Point location;

        public Double[] legacy;
        public String category;


        public Place(String name, Point location, Double[] legacy, String category) {
            this.name = name;
            this.location = location;
            this.legacy = legacy;
            this.category = category;
        }
    }
}
