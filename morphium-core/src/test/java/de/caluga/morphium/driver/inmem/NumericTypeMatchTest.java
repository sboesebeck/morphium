package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #342: integral query values must match integral stored values across
 * the wrapper types (Byte/Short/Integer/Long). {@code find({counter: 2})} used to return
 * nothing for a stored {@code 2L} - so a {@code long} entity field never matched its own
 * integer query literal, and after a dump/restore (where the JSON parser delivers every
 * number as {@code Long}) even {@code int} fields stopped answering integer queries.
 *
 * <p>The equivalence is deliberately INTEGRAL-ONLY: {@code Double}/{@code Float} and
 * {@code BigDecimal} stay out (precision questions - {@code 1.0} vs {@code 1} - and the
 * BigDecimal side is #334 symptom 2). {@link #doubleEquivalenceIsDeliberatelyNotIncluded}
 * pins that scope.
 *
 * <p>Covers every path that used to compare by wrapper type: the interpreted matcher
 * ({@code QueryHelper}), the compiled matcher ({@code CompiledQuery}, which {@code find} uses),
 * the compiled {@code $in}/{@code $nin} hash sets, the multikey list-contains branch, and -
 * critically - the index equality path ({@code IndexKey} as a {@code HashMap} key), so the fix
 * cannot silently shift the problem from the scan path into the index path.
 */
@Tag("inmemory")
public class NumericTypeMatchTest {
    private static final String DB = "numeric_match_db";
    private static final String COLL = "numeric_coll";

    private InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.setExpireCheck(3_600_000);
        drv.connect();
        return drv;
    }

    /** The issue's headline case, no restore involved: stored Long, queried with Integer. */
    @Test
    public void directEqualityMatchesAcrossIntegerAndLong() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("lng", 9L, "boxed", 7))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", 9), null, null, 0, 0).size(),
                    "#342: a stored Long must match its Integer query literal (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("boxed", 7L), null, null, 0, 0).size(),
                    "#342: a stored Integer must match a Long query value (compiled path)");
            assertTrue(QueryHelper.matchesQuery(Doc.of("lng", 9), Doc.of("lng", 9L), null),
                    "#342: the interpreted matcher must agree with the compiled one");
            assertTrue(QueryHelper.matchesQuery(Doc.of("boxed", 7L), Doc.of("boxed", 7), null),
                    "#342: interpreted matcher, Integer stored / Long queried");
        } finally {
            drv.close();
        }
    }

    /** Multikey branch: the document field is a list, matched via contains(). */
    @Test
    public void listContainsMatchesAcrossIntegerAndLong() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("vals", List.of(1L, 2L)))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("vals", 2), null, null, 0, 0).size(),
                    "#342: an Integer query value must match a Long list element (compiled path)");
            assertTrue(QueryHelper.matchesQuery(Doc.of("vals", 2), Doc.of("vals", List.of(1L, 2L)), null),
                    "#342: interpreted matcher, Long list element / Integer query");
        } finally {
            drv.close();
        }
    }

    /** The compiled $in/$nin path uses HashSet lookups, not compareValues - it must agree. */
    @Test
    public void compiledInAndNinMatchAcrossIntegerAndLong() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("counter", 2L))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("counter", Doc.of("$in", List.of(2))), null, null, 0, 0).size(),
                    "#342: $in [Integer] must match a stored Long (compiled set path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("counter", Doc.of("$nin", List.of(2))), null, null, 0, 0).size(),
                    "#342: $nin [Integer] must exclude a stored Long (compiled set path)");
        } finally {
            drv.close();
        }
    }

    /**
     * The index path (#342, coordinator point 2): with an index present, the equality lookup
     * goes through IndexKey as a HashMap key. An index built over Long values must answer an
     * Integer probe - otherwise the fix only moves the problem from the scan path into the
     * index path, where it depends on index existence and is much harder to spot.
     */
    @Test
    public void indexedEqualityLookupMatchesAcrossIntegerAndLong() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(
                        Doc.of("lng", 9L, "tag", "a"),
                        Doc.of("lng", 10L, "tag", "b"))).execute();
            drv.createIndex(DB, COLL, Doc.of("lng", 1), Doc.of("name", "lng_idx"));

            // warm the index store with a type-exact probe - this builds and caches it
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", 9L), null, null, 0, 0).size(),
                    "sanity: the type-exact probe must hit via the index");

            List<Map<String, Object>> found = drv.find(DB, COLL, Doc.of("lng", 9), null, null, 0, 0);
            assertEquals(1, found.size(),
                    "#342: an index built over Long values must answer an Integer probe");
        } finally {
            drv.close();
        }
    }

    /** Structural half of the index fix: cross-wrapper keys must be equal AND hash-equal. */
    @Test
    public void indexKeysNormalizeIntegralWrappers() {
        IndexKey intKey = IndexKey.of(List.of(9));
        IndexKey longKey = IndexKey.of(List.of(9L));
        assertEquals(intKey, longKey,
                "#342: IndexKey must normalize integral wrappers - Integer and Long probes must land in the same bucket");
        assertEquals(intKey.hashCode(), longKey.hashCode(),
                "#342: hash codes must agree or the HashMap lookup misses the bucket");
    }

    /** End to end: the issue's measurement protocol - after a restore everything is Long. */
    @Test
    public void restoredLongValuesAnswerIntegerQueries(@TempDir Path tmp) throws Exception {
        InMemoryDriver src = freshDriver();
        try {
            new InsertMongoCommand(src).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("counter", 2, "boxed", 7, "lng", 9L))).execute();
            File f = new File(tmp.toFile(), DB + ".morphium.gz");
            src.dumpToFile(DB, f);

            InMemoryDriver target = freshDriver();
            try {
                target.restoreFromFile(f);
                assertEquals(1, target.find(DB, COLL, Doc.of("counter", 2), null, null, 0, 0).size(),
                        "#342: counter==2 must hit after restore (issue protocol line: 'AFTER restore: counter==2 -> 0')");
                assertEquals(1, target.find(DB, COLL, Doc.of("boxed", 7), null, null, 0, 0).size(),
                        "#342: boxed==7 must hit after restore");
                assertEquals(1, target.find(DB, COLL, Doc.of("lng", 9), null, null, 0, 0).size(),
                        "#342: lng==9 must hit after restore");
            } finally {
                target.close();
            }
        } finally {
            src.close();
        }
    }

    /** Regression guard: the range operators already compare numerically - keep it that way. */
    @Test
    public void rangeOperatorsAlreadyMatchNumerically() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("counter", 2L))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("counter", Doc.of("$gt", 1)), null, null, 0, 0).size(),
                    "$gt with an Integer operand must match a stored Long");
            assertEquals(1, drv.find(DB, COLL, Doc.of("counter", Doc.of("$lte", 2)), null, null, 0, 0).size(),
                    "$lte with an Integer operand must match a stored Long");
            assertEquals(1, drv.find(DB, COLL, Doc.of("counter", Doc.of("$eq", 2)), null, null, 0, 0).size(),
                    "$eq with an Integer operand must match a stored Long");
        } finally {
            drv.close();
        }
    }

    /**
     * Scope pin (#342, deliberately narrow): the new equivalence covers INTEGRAL wrappers
     * only. A stored Double 2.0 keeps NOT matching a direct integer equality query - exactly
     * as before the fix. (The $eq/$in operator paths have compared all Numbers via
     * doubleValue() since long before #342; that pre-existing behavior is untouched here.)
     * Widening direct equality to floating point is a #334-adjacent decision, not a side
     * effect this fix is allowed to smuggle in.
     */
    @Test
    public void doubleEquivalenceIsDeliberatelyNotIncluded() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("dbl", 2.0d))).execute();

            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", 2), null, null, 0, 0).size(),
                    "direct integer equality against a stored Double stays unmatched - out of #342's scope");
            assertFalse(QueryHelper.matchesQuery(Doc.of("dbl", 2), Doc.of("dbl", 2.0d), null),
                    "interpreted matcher: same deliberate scope limit");
        } finally {
            drv.close();
        }
    }
}
