package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * MOR-037: {@code Morphium.simplifyQueryObject} flattens a query that consists of one {@code $and} into a single map (the writer does that for an upsert, so that the equalities
 * can seed the new document). It merged the maps of the branches with {@code putAll}, so a key that two branches share - two {@code $or}, two conditions on one field - overwrote the
 * earlier predicate and the update matched more than the query asked for. Branches that share a key with different predicates are not flattened now.
 */
@Tag("core")
public class SimplifyQueryObjectTest extends MultiDriverTestBase {

    @Entity
    public static class Pair {
        @Id
        public String id;
        public int k;
        public int m;
        public int v;
    }

    private static Map<String, Object> or(String field, int... values) {
        List<Object> branches = new ArrayList<>();

        for (int v : values) {
            branches.add(Doc.of(field, v));
        }

        return Doc.of("$or", branches);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void twoOrConditionsAreNotMergedIntoOne(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> q = Doc.of("$and", List.of(or("k", 1, 2), or("m", 1, 2)));
            assertSame(q, m.simplifyQueryObject(q), "branches that share $or stay an $and");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void twoConditionsOnOneFieldAreNotMergedIntoOne(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> q = Doc.of("$and", List.of(Doc.of("k", Doc.of("$gt", 1)), Doc.of("k", Doc.of("$lt", 5))));
            assertSame(q, m.simplifyQueryObject(q));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void branchesWithDifferentKeysAreStillFlattened(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> q = Doc.of("$and", List.of(Doc.of("_id", "x"), Doc.of("k", 2), or("m", 1)));
            assertEquals(Doc.of("_id", "x", "k", 2, "$or", List.of(Doc.of("m", 1))), m.simplifyQueryObject(q));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRepeatedKeyWithTheSamePredicateIsFlattened(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> q = Doc.of("$and", List.of(Doc.of("k", 2), Doc.of("k", 2), Doc.of("m", 3)));
            assertEquals(Doc.of("k", 2, "m", 3), m.simplifyQueryObject(q));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpsertWithTwoOrConditionsUpdatesOnlyWhatBothMatch(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Pair.class);

            for (int[] km : new int[][] {{1, 1}, {1, 9}, {9, 1}, {2, 2}}) {
                Pair p = new Pair();
                p.id = km[0] + "-" + km[1];
                p.k = km[0];
                p.m = km[1];
                m.store(p);
            }

            Map<String, Object> filter = Doc.of("$and", List.of(or("k", 1, 2), or("m", 1, 2)));
            m.createQueryFor(Pair.class).rawQuery(filter).inc("v", 7, true, true);
            assertEquals(List.of("1-1", "2-2"), m.createQueryFor(Pair.class).f("v").eq(7).sort("id").asList().stream().map(p -> p.id).toList(),
                         "only the documents that match both $or conditions are updated");
            assertEquals(4, m.createQueryFor(Pair.class).countAll(), "nothing was inserted: the filter matched");
        }
    }
}
