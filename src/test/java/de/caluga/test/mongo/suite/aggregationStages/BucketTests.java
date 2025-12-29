package de.caluga.test.mongo.suite.aggregationStages;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.List;

import static de.caluga.morphium.aggregation.Expr.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@Tag("aggregation")
public class BucketTests extends MultiDriverTestBase {


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void bucketTest(Morphium morphium) throws Exception  {
        morphium.clearCollection(Artist.class);
        Thread.sleep(100);
        morphium.store(new Artist("Bernard", "Emil", 1868, 1941, "France"));
        morphium.store(new Artist("Rippl-Ronai", "Joszef", 1861, 1927, "Hungary"));
        morphium.store(new Artist("Ostroumova", "Anna", 1871, 1955, "Russia"));
        morphium.store(new Artist("Van Gogh", "Vincent", 1853, 1890, "Holland"));
        morphium.store(new Artist("Maurer", "Alfred", 1868, 1932, "USA"));
        morphium.store(new Artist("Munch", "Edvard", 1863, 1944, "Norway"));
        morphium.store(new Artist("Redon", "Odilon", 1840, 1916, "France"));
        morphium.store(new Artist("Diriks", "Edvard", 1855, 1930, "Norway"));


        Aggregator<Artist, ArtistAggregation> agg = morphium.createAggregator(Artist.class, ArtistAggregation.class);

        agg.bucket(field("year_born"), Arrays.asList(intExpr(1840), intExpr(1850), intExpr(1860), intExpr(1870), intExpr(1880)), string("Other"), UtilsMap.of("count", sum(intExpr(1)), "artists", push(mapExpr(UtilsMap.of("name", concat(field("first_name"), string(" "), field("last_name")), "year_born", field("year_born"))))));

        List<ArtistAggregation> lst = agg.aggregate();
        for (ArtistAggregation a : lst) {
            log.info(a.toString());

            assertNotNull(a.artists);
            ;
            assert (a.artists.size() > 0);
            assert (a.count == a.artists.size());
            for (Artist artist : a.artists) {
                assert (a.id <= artist.yearBorn);
            }
        }


    }

    @Entity
    public static class ArtistAggregation {
        @Id
        public Integer id;
        public Integer count;
        public List<Artist> artists;

        @Override
        public String toString() {
            return "ArtistAggregation{" + "id=" + id + ", count=" + count + ", artists=" + toString(artists) + '}';
        }


        private String toString(List l) {
            StringBuilder b = new StringBuilder();
            b.append("[ ");
            for (Object o : l) {
                b.append(o.toString());
                b.append(",");
            }
            b.append(" ] ");

            return b.toString();
        }
    }

    @Entity
    public static class Artist {
        @Id
        public MorphiumId id;
        public String firstName, lastName, name;
        public Integer yearBorn, yearDied;
        public String nationality;

        public Artist(String firstName, String lastName, Integer yearBorn, Integer yearDied, String nationality) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.yearBorn = yearBorn;
            this.yearDied = yearDied;
            this.nationality = nationality;
        }

        @Override
        public String toString() {
            return "Artist{" + "Name='" + name + '\'' + ", yearBorn=" + yearBorn + '}';
        }
    }
}
