package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * MOR-006: {@code findOneAndUpdate} passes the array filters and the sort of its {@link Query} to the server (findAndModify supports both). Before, the
 * array filters of the query were ignored by this call and the InMemoryDriver dropped them for findAndModify.
 */
@Tag("core")
public class FindOneAndUpdateArrayFiltersAndSortTest extends MultiDriverTestBase {

    @Embedded(translateCamelCase = false)
    public static class Score {
        public String name;
        public int points;

        public Score() {
        }

        public Score(String name, int points) {
            this.name = name;
            this.points = points;
        }
    }

    @Entity(translateCamelCase = false)
    public static class Player {
        @Id
        public MorphiumId id;
        public String team;
        public int rank;
        public List<Score> scores;
        public List<Integer> values;
        public String status;
    }

    private Player player(Morphium m, String team, int rank, int... values) {
        Player p = new Player();
        p.team = team;
        p.rank = rank;
        p.values = new ArrayList<>();

        for (int v : values) {
            p.values.add(v);
        }

        p.scores = new ArrayList<>(List.of(new Score("a", 10), new Score("b", 90), new Score("c", 95)));
        m.store(p);
        return p;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayFiltersOfTheQueryAreAppliedByFindOneAndUpdate(Morphium m) throws Exception {
        try (m) {
            player(m, "t", 1, 50, 95, 99);
            Query<Player> q = m.createQueryFor(Player.class).f("team").eq("t")
                .setArrayFilters(Doc.of("elem", Doc.of("$gte", 90)));
            Player before = q.findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("values.$[elem]", 0)), false, false);
            assertNotNull(before);
            assertEquals(List.of(50, 95, 99), before.values);
            Player after = m.createQueryFor(Player.class).f("team").eq("t").get();
            assertEquals(List.of(50, 0, 0), after.values);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayFiltersOnEmbeddedDocumentsAndWithReturnNew(Morphium m) throws Exception {
        try (m) {
            player(m, "e", 1);
            Query<Player> q = m.createQueryFor(Player.class).f("team").eq("e")
                .setArrayFilters(Doc.of("s.points", Doc.of("$gte", 90)));
            Player after = q.findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("scores.$[s].name", "top")), false, true);
            assertEquals(List.of("a", "top", "top"), after.scores.stream().map(s -> s.name).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theLegacyOneArgumentOverloadAppliesArrayFiltersToo(Morphium m) throws Exception {
        try (m) {
            player(m, "l", 1, 10, 20);
            Query<Player> q = m.createQueryFor(Player.class).f("team").eq("l")
                .setArrayFilters(Doc.of("elem", Doc.of("$lt", 15)));
            assertNotNull(q.findOneAndUpdate(UtilsMap.of("$inc", UtilsMap.of("values.$[elem]", 1))));
            assertEquals(List.of(11, 20), m.createQueryFor(Player.class).f("team").eq("l").get().values);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortPicksTheDocumentThatIsUpdated(Morphium m) throws Exception {
        try (m) {
            player(m, "s", 3);
            player(m, "s", 1);
            player(m, "s", 2);
            Query<Player> q = m.createQueryFor(Player.class).f("team").eq("s").sort("rank");
            Player before = q.findOneAndUpdate(Map.of("$set", Map.of("status", "taken")), false, false);
            assertEquals(1, before.rank, "ascending sort: the lowest rank is updated");
            List<Player> all = m.createQueryFor(Player.class).f("team").eq("s").sort("rank").asList();
            assertEquals(1, all.stream().filter(p -> "taken".equals(p.status)).count(), "exactly one document is updated");
            assertEquals("taken", all.get(0).status);

            Player desc = m.createQueryFor(Player.class).f("team").eq("s").sort("-rank").findOneAndUpdate(Map.of("$set", Map.of("status", "taken")), false, true);
            assertEquals(3, desc.rank, "descending sort: the highest rank is updated");
        }
    }
}
