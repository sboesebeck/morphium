package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static de.caluga.morphium.aggregation.Expr.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class BucketAutoTests extends MorphiumTestBase {

    @Test
    public void bucketAutoTest() throws Exception {
        prepData();
        Aggregator<Artwork, Map> agg = morphium.createAggregator(Artwork.class, Map.class);
        agg.bucketAuto(field("price"), 4, null, null);
        List<Map> list = agg.aggregate();
        Double lastMax = null;

        for (Map m : list) {
            log.info("Entry: " + m.toString());
            assert(m.get("count").equals(2));
            Double max = (Double)((Map) m.get("_id")).get("max");
            Double min = (Double)((Map) m.get("_id")).get("min");
            assertNotNull(min);
            ;
            assertNotNull(max);
            ;

            if (lastMax != null) {
                assert(min.equals(lastMax));
            }

            lastMax = max;
        }
    }


    @Test
    public void bucketAutoFacets() throws Exception {
        prepData();
        Aggregator<Artwork, Map> priceAggregator = morphium.createAggregator(Artwork.class, Map.class);
        priceAggregator.bucketAuto(field("price"), 4, null, null);
        Aggregator<Artwork, Map> yearAggregator = morphium.createAggregator(Artwork.class, Map.class);
        yearAggregator.bucketAuto(field("year"), 3, UtilsMap.of("count", sum(intExpr(1)), "years", push(field("year"))), null);
        Aggregator<Artwork, Map> areaAggregator = morphium.createAggregator(Artwork.class, Map.class);
        areaAggregator.bucketAuto(multiply(field("dimensions.height"), field("dimensions.width")), 4, UtilsMap.of("count", sum(intExpr(1)), "titles", push(field("title"))), null);
        Aggregator<Artwork, Map> aggregator = morphium.createAggregator(Artwork.class, Map.class);
        aggregator.facet(UtilsMap.of("price", (Aggregator) priceAggregator, "year", yearAggregator, "area", areaAggregator)
        );
        List<Map> list = aggregator.aggregate();
        assertEquals(1, list.size());
        Map result = list.get(0);
        assertNotNull(result.get("area"));
        log.info("Result for Area:");
        log.info(result.get("area").toString());
        assertEquals(4, ((List) result.get("area")).size());
        // assertEquals("[{_id={min=432, max=500}, count=3, titles=[The Scream, The Persistence of Memory, Blue Flower]}, {_id={min=500, max=864}, count=2, titles=[Dancer, The Pillars of Society]}, {_id={min=864, max=1568}, count=2, titles=[The Great Wave off Kanagawa, Composition VII]}, {_id={min=1568, max=1568}, count=1, titles=[Melancholy III]}]",
        // result.get("area").toString());
        assertNotNull(result.get("price"));
        log.info("Result for price:");
        log.info(result.get("price").toString());
        assertEquals(4, ((List) result.get("price")).size());
        // assertEquals("[{_id={min=76.04, max=159.0}, count=2}, {_id={min=159.0, max=199.99}, count=2}, {_id={min=199.99, max=385.0}, count=2}, {_id={min=385.0, max=483.0}, count=2}]",
        // result.get("price").toString());
        assertNotNull(result.get("year"));
        log.info("Result for year:");
        log.info(result.get("year").toString());
        assertEquals(3, ((List) result.get("year")).size());
        // assertEquals("[{_id={min=null, max=1913}, count=3, years=[1902]}, {_id={min=1913, max=1926}, count=3, years=[1913, 1918, 1925]}, {_id={min=1926, max=1931}, count=2, years=[1926, 1931]}]",
        //     result.get("year").toString());
    }

    private void prepData() throws Exception {
        morphium.clearCollection(Artwork.class);
        Thread.sleep(100);
        morphium.store(new Artwork("The Pillars of Society", "Grosz", 1926,
            199.99,
            39, 21, "in"));
        morphium.store(new Artwork("Melancholy III", "Munch", 1902,
            280.00,
            49, 32, "in"));
        morphium.store(new Artwork("Dancer", "Miro", 1925,
            76.04,
            25, 20, "in"));
        morphium.store(new Artwork("The Great Wave off Kanagawa", "Hokusai", null,
            167.30,
            24, 36, "in"));
        morphium.store(new Artwork("The Persistence of Memory", "Dali", 1931,
            483.00,
            20, 24, "in"));
        morphium.store(new Artwork("Composition VII", "Kandinsky", 1913,
            385.00,
            30, 46, "in"));
        morphium.store(new Artwork("The Scream", "Munch", null,
            159.00,
            24, 18, "in"));
        morphium.store(new Artwork("Blue Flower", "O'Keefe", 1918,
            118.42,
            24, 20, "in"));
        Thread.sleep(1000);
    }


    @Entity
    public static class Artwork {
        @Id
        public MorphiumId id;

        public String title;
        public String artist;
        public Double price;
        public Integer year;
        public Dimension dimensions;

        public Artwork(String title, String artist, Integer year, Double price, Integer width, Integer height, String unit) {
            this.title = title;
            this.artist = artist;
            this.price = price;
            this.year = year;
            this.dimensions = new Dimension(width, height, unit);
        }
    }


    @Embedded
    public static class Dimension {
        public Integer width, height;
        public String unit;

        public Dimension(Integer width, Integer height, String unit) {
            this.width = width;
            this.height = height;
            this.unit = unit;
        }
    }
}
