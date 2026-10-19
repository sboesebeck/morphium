package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MOR-067: {@code Query.toQueryObject} built the {@code $or} list and then the {@code $nor} list in the same list variable, so a query with {@code or()} and {@code nor()} became
 * {@code {$or: [A, B], $nor: [A, B]}}, which matches nothing, instead of {@code {$or: [A], $nor: [B]}}.
 */
@Tag("core")
public class QueryOrNorTest extends MultiDriverTestBase {

    @Entity
    public static class Item {
        @Id
        public String id;
        public int a;
        public int b;
        public int c;
    }

    private Query<Item> q(Morphium m) {
        return m.createQueryFor(Item.class);
    }

    private void fill(Morphium m) {
        m.dropCollection(Item.class);

        // a: 1 or 2 wanted, b: 9 unwanted
        for (int[] v : new int[][] {{1, 0, 0}, {2, 0, 0}, {1, 9, 0}, {3, 0, 0}, {2, 9, 5}}) {
            Item i = new Item();
            i.id = v[0] + "-" + v[1] + "-" + v[2];
            i.a = v[0];
            i.b = v[1];
            i.c = v[2];
            m.store(i);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void orAndNorKeepTheirOwnQueries(Morphium m) throws Exception {
        try (m) {
            Query<Item> query = q(m);
            query.or(q(m).f("a").eq(1), q(m).f("a").eq(2));
            query.nor(q(m).f("b").eq(9));
            assertEquals(Doc.of("$or", List.of(Doc.of("a", 1), Doc.of("a", 2)), "$nor", List.of(Doc.of("b", 9))), query.toQueryObject());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void orAndNorMatchTheRightDocuments(Morphium m) throws Exception {
        try (m) {
            fill(m);
            Query<Item> query = q(m);
            query.or(q(m).f("a").eq(1), q(m).f("a").eq(2));
            query.nor(q(m).f("b").eq(9));
            assertEquals(List.of("1-0-0", "2-0-0"), query.sort("id").asList().stream().map(i -> i.id).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void withAnAndConditionBothStayInsideTheAnd(Morphium m) throws Exception {
        try (m) {
            Query<Item> query = q(m).f("c").eq(0);
            query.or(q(m).f("a").eq(1), q(m).f("a").eq(2));
            query.nor(q(m).f("b").eq(9));
            Map<String, Object> o = query.toQueryObject();
            assertEquals(Doc.of("$and", List.of(Doc.of("c", 0), Doc.of("$or", List.of(Doc.of("a", 1), Doc.of("a", 2))), Doc.of("$nor", List.of(Doc.of("b", 9))))), o);

            fill(m);
            assertEquals(List.of("1-0-0", "2-0-0"), query.sort("id").asList().stream().map(i -> i.id).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aQueryWithOnlyOrOrOnlyNorIsUnchanged(Morphium m) throws Exception {
        try (m) {
            Query<Item> onlyOr = q(m);
            onlyOr.or(q(m).f("a").eq(1), q(m).f("a").eq(2));
            assertEquals(Doc.of("$or", List.of(Doc.of("a", 1), Doc.of("a", 2))), onlyOr.toQueryObject());
            Query<Item> onlyNor = q(m);
            onlyNor.nor(q(m).f("b").eq(9), q(m).f("c").eq(5));
            assertEquals(Doc.of("$nor", List.of(Doc.of("b", 9), Doc.of("c", 5))), onlyNor.toQueryObject());
        }
    }
}
