package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #342: integral query values must match integral stored values across
 * the wrapper types (Byte/Short/Integer/Long). {@code find({counter: 2})} used to return
 * nothing for a stored {@code 2L} - so a {@code long} entity field never matched its own
 * integer query literal, and after a dump/restore (where the JSON parser delivers every
 * number as {@code Long}) even {@code int} fields stopped answering integer queries.
 *
 * <p>#344 extends the equivalence to {@code Double}: {@code {x: 2}} and {@code {x: {$eq: 2}}}
 * used to disagree on a stored {@code 2.0}, because only the operator path compared
 * numerically. The cross-type rule is EXACT, never a {@code doubleValue()} cast: a Long only
 * equals a Double whose value is an integer that converts back to the very same Long, so
 * {@code 9007199254740993L} (2^53+1, no exact double form) must NOT match
 * {@code 9007199254740992.0}. {@code BigDecimal} stays out (#343, decimal128 there).
 *
 * <p>#379 closes the remaining gap, the OPERATOR path: {@code $eq}/{@code $ne}/{@code $lt}..
 * {@code $gte}, the interpreted {@code $in} and {@code listEquals} compared every Number via
 * {@code doubleValue()} - including Long against Long - so past 2^53 adjacent longs collapsed
 * ({@code {$eq: 2^53+1}} matched a stored 2^53, {@code {$lt: 2^53+1}} missed it). Ordering
 * cannot be fixed by a cast, so the comparison is a cascade (Long vs Long exact, Long vs Double
 * exact by value, Double vs Double as before), and the {@code IndexKey} TreeMap comparator
 * follows it so an index range scan and a collscan cannot disagree.
 *
 * <p>Covers every path that used to compare by wrapper type: the interpreted matcher
 * ({@code QueryHelper}), the compiled matcher ({@code CompiledQuery}, which {@code find} uses),
 * the compiled {@code $in}/{@code $nin} hash sets, the {@code $all} hash sets of both matchers,
 * the multikey list-contains branch, and -
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

    /** #344 headline case: stored Double 2.0, queried with the integer literal 2 - and back. */
    @Test
    public void directEqualityMatchesAcrossIntegerAndDouble() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("dbl", 2.0d, "boxed", 3, "lng", 4L))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", 2), null, null, 0, 0).size(),
                    "#344: a stored Double must match its Integer query literal (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", 2L), null, null, 0, 0).size(),
                    "#344: a stored Double must match a Long query value (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("boxed", 3.0d), null, null, 0, 0).size(),
                    "#344: a stored Integer must match a Double query value (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", 4.0d), null, null, 0, 0).size(),
                    "#344: a stored Long must match a Double query value (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", 3), null, null, 0, 0).size(),
                    "sanity: a different integer must still not match");

            assertTrue(QueryHelper.matchesQuery(Doc.of("dbl", 2), Doc.of("dbl", 2.0d), null),
                    "#344: the interpreted matcher must agree with the compiled one");
            assertTrue(QueryHelper.matchesQuery(Doc.of("boxed", 3.0d), Doc.of("boxed", 3), null),
                    "#344: interpreted matcher, Integer stored / Double queried");
            assertTrue(QueryHelper.matchesQuery(Doc.of("lng", 4.0d), Doc.of("lng", 4L), null),
                    "#344: interpreted matcher, Long stored / Double queried");
        } finally {
            drv.close();
        }
    }

    /** The issue's literal complaint: the two spellings of one query must return the same rows. */
    @Test
    public void directEqualityAndEqOperatorAgreeOnStoredDouble() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("dbl", 2.0d), Doc.of("dbl", 2.5d))).execute();

            int viaOperator = drv.find(DB, COLL, Doc.of("dbl", Doc.of("$eq", 2)), null, null, 0, 0).size();
            int viaDirect = drv.find(DB, COLL, Doc.of("dbl", 2), null, null, 0, 0).size();
            assertEquals(1, viaOperator, "sanity: the operator path matched 2.0 numerically before #344");
            assertEquals(viaOperator, viaDirect,
                    "#344: {dbl: 2} and {dbl: {$eq: 2}} must return the same rows");
        } finally {
            drv.close();
        }
    }

    /** Multikey branch: the document field is a list of doubles, probed with an integer. */
    @Test
    public void listContainsMatchesAcrossIntegerAndDouble() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("vals", List.of(1.0d, 2.0d)), Doc.of("ints", List.of(5, 6)))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("vals", 2), null, null, 0, 0).size(),
                    "#344: an Integer query value must match a Double list element (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("ints", 6.0d), null, null, 0, 0).size(),
                    "#344: a Double query value must match an Integer list element (compiled path)");
            assertTrue(QueryHelper.matchesQuery(Doc.of("vals", 2), Doc.of("vals", List.of(1.0d, 2.0d)), null),
                    "#344: interpreted matcher, Double list element / Integer query");
            assertTrue(QueryHelper.matchesQuery(Doc.of("ints", 6.0d), Doc.of("ints", List.of(5, 6)), null),
                    "#344: interpreted matcher, Integer list element / Double query");
        } finally {
            drv.close();
        }
    }

    /** The compiled $in/$nin HashSets need canonical values - both directions. */
    @Test
    public void compiledInAndNinMatchAcrossIntegerAndDouble() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("dbl", 2.0d, "lng", 3L))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$in", List.of(1, 2))), null, null, 0, 0).size(),
                    "#344: $in [Integer] must match a stored Double (compiled set path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$nin", List.of(2))), null, null, 0, 0).size(),
                    "#344: $nin [Integer] must exclude a stored Double (compiled set path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", Doc.of("$in", List.of(3.0d))), null, null, 0, 0).size(),
                    "#344: $in [Double] must match a stored Long (compiled set path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("lng", Doc.of("$nin", List.of(3.0d))), null, null, 0, 0).size(),
                    "#344: $nin [Double] must exclude a stored Long (compiled set path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$in", List.of(2.5d, 3))), null, null, 0, 0).size(),
                    "sanity: $in without an equal member must still miss");
        } finally {
            drv.close();
        }
    }

    /** Index path: an index built over doubles must answer an integer probe, and vice versa. */
    @Test
    public void indexedEqualityLookupMatchesAcrossIntegerAndDouble() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(
                        Doc.of("dbl", 9.0d, "lng", 90L),
                        Doc.of("dbl", 10.0d, "lng", 100L),
                        Doc.of("dbl", 10.5d, "lng", 105L))).execute();
            drv.createIndex(DB, COLL, Doc.of("dbl", 1), Doc.of("name", "dbl_idx"));
            drv.createIndex(DB, COLL, Doc.of("lng", 1), Doc.of("name", "lng_idx"));

            // warm both index stores with type-exact probes
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", 9.0d), null, null, 0, 0).size(),
                    "sanity: the type-exact double probe must hit via the index");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", 90L), null, null, 0, 0).size(),
                    "sanity: the type-exact long probe must hit via the index");

            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", 9), null, null, 0, 0).size(),
                    "#344: an index built over Double values must answer an Integer probe");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", 10L), null, null, 0, 0).size(),
                    "#344: an index built over Double values must answer a Long probe");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", 90.0d), null, null, 0, 0).size(),
                    "#344: an index built over Long values must answer a Double probe");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$in", List.of(10, 11))), null, null, 0, 0).size(),
                    "#344: indexed $in with integer members must find the 10.0 row only, not 10.5");
        } finally {
            drv.close();
        }
    }

    /** Structural half: an exactly-integral Double and its Long must be equal AND hash-equal keys. */
    @Test
    public void indexKeysCanonicalizeExactDoubles() {
        assertEquals(IndexKey.of(List.of(9)), IndexKey.of(List.of(9.0d)),
                "#344: Integer and Double probes of one value must land in the same bucket");
        assertEquals(IndexKey.of(List.of(9)).hashCode(), IndexKey.of(List.of(9.0d)).hashCode(),
                "#344: hash codes must agree or the HashMap lookup misses the bucket");
        assertEquals(IndexKey.of(List.of(9L)), IndexKey.of(List.of(9.0d)),
                "#344: Long and Double keys of one value must be equal");
        assertNotEquals(IndexKey.of(List.of(9)), IndexKey.of(List.of(9.5d)),
                "a non-integral Double keeps its own bucket");
        assertNotEquals(IndexKey.of(List.of(9007199254740993L)), IndexKey.of(List.of(9007199254740992.0d)),
                "#344: 2^53+1 has no exact double form - the keys must NOT collapse");
        assertNotEquals(IndexKey.of(List.of(Long.MAX_VALUE)), IndexKey.of(List.of(9.223372036854775807E18d)),
                "#344: Long.MAX_VALUE rounds to 2^63 as a double - the keys must NOT collapse");
    }

    /**
     * The exactness rule (#344): equality is decided on the exact mathematical value, never
     * through a {@code doubleValue()} cast. 2^53+1 as a Long has no double representation; the
     * nearest double is 2^53. Reporting those as equal would turn a missed match into a wrong
     * match, which is worse than the bug being fixed.
     */
    @Test
    public void longsBeyondDoublePrecisionDoNotMatchTheirNearestDouble() throws Exception {
        long beyond = 9007199254740993L;          // 2^53 + 1
        double nearest = 9007199254740992.0d;     // 2^53 - what (double) beyond yields
        assertEquals(nearest, (double) beyond, "test premise: the cast really collapses");

        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("lng", beyond), Doc.of("dbl", nearest), Doc.of("max", Long.MAX_VALUE))).execute();

            assertEquals(0, drv.find(DB, COLL, Doc.of("lng", nearest), null, null, 0, 0).size(),
                    "#344: a stored 2^53+1 must NOT match the double 2^53 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", beyond), null, null, 0, 0).size(),
                    "#344: a stored double 2^53 must NOT match the long 2^53+1 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("max", 9.223372036854775807E18d), null, null, 0, 0).size(),
                    "#344: Long.MAX_VALUE must NOT match the double 2^63 it rounds to (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("lng", Doc.of("$in", List.of(nearest))), null, null, 0, 0).size(),
                    "#344: compiled $in must apply the same exactness rule");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", beyond), null, null, 0, 0).size(),
                    "sanity: the exact Long probe still hits its own row");
        } finally {
            drv.close();
        }

        assertFalse(QueryHelper.matchesQuery(Doc.of("lng", nearest), Doc.of("lng", beyond), null),
                "#344: interpreted matcher, stored 2^53+1 / queried 2^53 double");
        assertFalse(QueryHelper.matchesQuery(Doc.of("dbl", beyond), Doc.of("dbl", nearest), null),
                "#344: interpreted matcher, stored 2^53 double / queried 2^53+1 long");
        assertFalse(QueryHelper.matchesQuery(Doc.of("max", 9.223372036854775807E18d), Doc.of("max", Long.MAX_VALUE), null),
                "#344: interpreted matcher, Long.MAX_VALUE / double 2^63");
        assertFalse(QueryHelper.matchesQuery(Doc.of("dbl", 2), Doc.of("dbl", 2.5d), null),
                "sanity: a non-integral double never equals an integer");
    }
    /**
     * {@code $all} in both matchers used a raw {@code HashSet} of the stored list and looked the
     * operand members up by {@code equals()}, so neither #342 nor #344 reached it: {@code {$all:
     * [2]}} missed a stored {@code [2L]} and a stored {@code [2.0]} while {@code $in} hit both.
     * Both sides now go through the same canonicalization as the {@code $in}/{@code $nin} sets.
     */
    @Test
    public void allMatchesAcrossNumericWrappers() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("lngs", List.of(2L, 3L), "dbls", List.of(2.0d, 3.0d), "ints", List.of(2, 3)))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("lngs", Doc.of("$all", List.of(2))), null, null, 0, 0).size(),
                    "#342-gap: $all [Integer] must match a stored Long list (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbls", Doc.of("$all", List.of(2))), null, null, 0, 0).size(),
                    "#344-gap: $all [Integer] must match a stored Double list (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("ints", Doc.of("$all", List.of(2.0d, 3L))), null, null, 0, 0).size(),
                    "$all [Double, Long] must match a stored Integer list (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lngs", Doc.of("$all", List.of(2.0d))), null, null, 0, 0).size(),
                    "$all [Double] must match a stored Long list (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbls", Doc.of("$all", List.of(2, 4))), null, null, 0, 0).size(),
                    "sanity: $all still requires EVERY member to be present");
            assertEquals(0, drv.find(DB, COLL, Doc.of("ints", Doc.of("$all", List.of(2.5d))), null, null, 0, 0).size(),
                    "sanity: a non-integral double never equals an integer element");

            assertTrue(QueryHelper.matchesQuery(Doc.of("lngs", Doc.of("$all", List.of(2))), Doc.of("lngs", List.of(2L, 3L)), null),
                    "interpreted matcher: $all [Integer] / stored Long list");
            assertTrue(QueryHelper.matchesQuery(Doc.of("dbls", Doc.of("$all", List.of(2))), Doc.of("dbls", List.of(2.0d, 3.0d)), null),
                    "interpreted matcher: $all [Integer] / stored Double list");
            assertTrue(QueryHelper.matchesQuery(Doc.of("ints", Doc.of("$all", List.of(2.0d, 3L))), Doc.of("ints", List.of(2, 3)), null),
                    "interpreted matcher: $all [Double, Long] / stored Integer list");
            assertFalse(QueryHelper.matchesQuery(Doc.of("ints", Doc.of("$all", List.of(2.5d))), Doc.of("ints", List.of(2, 3)), null),
                    "interpreted matcher: non-integral double stays unequal");
        } finally {
            drv.close();
        }
    }

    /** $all and $in must agree member by member on a mixed-type list, in both matchers. */
    @Test
    public void allAndInAgreeOnMixedTypeLists() throws Exception {
        List<Object> stored = List.of(1, 2.0d, 3L);
        List<Object> probes = List.of(1L, 1.0d, 2, 2L, 3, 3.0d, 4, 2.5d);
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("mixed", stored))).execute();

            for (Object probe : probes) {
                int viaIn = drv.find(DB, COLL, Doc.of("mixed", Doc.of("$in", List.of(probe))), null, null, 0, 0).size();
                int viaAll = drv.find(DB, COLL, Doc.of("mixed", Doc.of("$all", List.of(probe))), null, null, 0, 0).size();
                assertEquals(viaIn, viaAll, "$all and $in must agree on probe " + probe + " (" + probe.getClass().getSimpleName() + ")");
                boolean interpreted = QueryHelper.matchesQuery(Doc.of("mixed", Doc.of("$all", List.of(probe))), Doc.of("mixed", stored), null);
                assertEquals(viaAll == 1, interpreted, "interpreted $all must agree with compiled $all on probe " + probe);
            }
            assertEquals(1, drv.find(DB, COLL, Doc.of("mixed", Doc.of("$all", List.of(1L, 2, 3.0d))), null, null, 0, 0).size(),
                    "$all with every member spelled in a different wrapper than stored must hit");
        } finally {
            drv.close();
        }
    }

    /** The exactness rule applies to $all as well: 2^53+1 as Long never equals the double 2^53. */
    @Test
    public void allKeepsLongsBeyondDoublePrecisionApart() throws Exception {
        long beyond = 9007199254740993L;
        double nearest = 9007199254740992.0d;
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("lngs", List.of(beyond)), Doc.of("dbls", List.of(nearest)))).execute();

            assertEquals(0, drv.find(DB, COLL, Doc.of("lngs", Doc.of("$all", List.of(nearest))), null, null, 0, 0).size(),
                    "compiled $all: stored 2^53+1 must NOT match the double 2^53");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbls", Doc.of("$all", List.of(beyond))), null, null, 0, 0).size(),
                    "compiled $all: stored double 2^53 must NOT match the long 2^53+1");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lngs", Doc.of("$all", List.of(beyond))), null, null, 0, 0).size(),
                    "sanity: the exact Long probe still hits");
        } finally {
            drv.close();
        }
        assertFalse(QueryHelper.matchesQuery(Doc.of("lngs", Doc.of("$all", List.of(nearest))), Doc.of("lngs", List.of(beyond)), null),
                "interpreted $all: stored 2^53+1 / probe double 2^53");
        assertFalse(QueryHelper.matchesQuery(Doc.of("dbls", Doc.of("$all", List.of(beyond))), Doc.of("dbls", List.of(nearest)), null),
                "interpreted $all: stored double 2^53 / probe long 2^53+1");
    }

    // ------------------------------------------------------------------------------------------
    // #379: the OPERATOR path ($eq/$ne/$lt/$lte/$gt/$gte, interpreted $in, listEquals) and the
    // index comparator must be exact past 2^53 too. After #342/#344 the direct-equality path was
    // exact while the operator path still compared every Number via doubleValue(), so adjacent
    // longs collapsed: {$eq: 2^53+1} matched a stored 2^53, {$lt: 2^53+1} missed it.
    // ------------------------------------------------------------------------------------------

    private static final long TWO_POW_53 = 9007199254740992L;

    /**
     * The issue's headline: every comparison operator, Long against Long, on the three adjacent
     * values 2^53, 2^53+1, 2^53+2. Only 2^53 and 2^53+2 have a double form, so via doubleValue()
     * 2^53+1 collapses onto 2^53 - {$eq} hits one row too many, {$lt} one too few. Both matchers
     * (compiled via find, interpreted via matchesQuery) must give the exact answer.
     */
    @Test
    public void operatorPathComparesLongsExactlyBeyondDoublePrecision() throws Exception {
        long p = TWO_POW_53;
        assertEquals((double) p, (double) (p + 1), "test premise: 2^53+1 has no exact double form");

        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("lng", p), Doc.of("lng", p + 1), Doc.of("lng", p + 2))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", Doc.of("$eq", p + 1)), null, null, 0, 0).size(),
                    "#379: {$eq: 2^53+1} must match exactly the 2^53+1 row, not the 2^53 row too (compiled path)");
            assertEquals(2, drv.find(DB, COLL, Doc.of("lng", Doc.of("$ne", p + 1)), null, null, 0, 0).size(),
                    "#379: {$ne: 2^53+1} must keep the 2^53 row (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", Doc.of("$lt", p + 1)), null, null, 0, 0).size(),
                    "#379: {$lt: 2^53+1} must find the stored 2^53 (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", Doc.of("$lte", p)), null, null, 0, 0).size(),
                    "#379: {$lte: 2^53} must NOT include the stored 2^53+1 (compiled path)");
            assertEquals(2, drv.find(DB, COLL, Doc.of("lng", Doc.of("$gt", p)), null, null, 0, 0).size(),
                    "#379: {$gt: 2^53} must find the stored 2^53+1 (compiled path)");
            assertEquals(2, drv.find(DB, COLL, Doc.of("lng", Doc.of("$gte", p + 1)), null, null, 0, 0).size(),
                    "#379: {$gte: 2^53+1} must NOT include the stored 2^53 (compiled path)");
        } finally {
            drv.close();
        }

        Map<String, Object> stored = Doc.of("lng", p);
        assertFalse(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$eq", p + 1)), stored, null),
                "#379: interpreted $eq must not collapse 2^53+1 onto a stored 2^53");
        assertTrue(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$ne", p + 1)), stored, null),
                "#379: interpreted $ne must keep a stored 2^53 for 2^53+1");
        assertTrue(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$lt", p + 1)), stored, null),
                "#379: interpreted {$lt: 2^53+1} must find a stored 2^53");
        assertFalse(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$gte", p + 1)), stored, null),
                "#379: interpreted {$gte: 2^53+1} must not include a stored 2^53");
        assertTrue(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$gt", p)), Doc.of("lng", p + 1), null),
                "#379: interpreted {$gt: 2^53} must find a stored 2^53+1");
        assertFalse(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$in", List.of(p + 1))), stored, null),
                "#379: the interpreted $in goes through compareValues and must be exact as well");
        assertTrue(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$in", List.of(p, p + 2))), stored, null),
                "sanity: interpreted $in still hits an exact member");
    }

    /**
     * Long against Double ORDERING past 2^53 - the reason this is a cascade and not a cast: a
     * stored 2^53+1 is strictly greater than the double 2^53, and Long.MAX_VALUE is strictly
     * less than the double 2^63 it rounds to. Via doubleValue() both pairs compare EQUAL, so the
     * strict operators miss and the inclusive ones hit the wrong way round. Equality stays the
     * #344 rule (no cross-type hit for either pair).
     */
    @Test
    public void operatorPathOrdersLongAgainstDoubleExactlyBeyondDoublePrecision() throws Exception {
        long beyond = TWO_POW_53 + 1;
        double nearest = (double) TWO_POW_53;      // 9007199254740992.0
        double twoPow63 = 0x1p63;                  // (double) Long.MAX_VALUE rounds up to this

        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("lng", beyond), Doc.of("max", Long.MAX_VALUE), Doc.of("dbl", nearest))).execute();

            // stored Long 2^53+1 vs. query Double 2^53
            assertEquals(1, drv.find(DB, COLL, Doc.of("lng", Doc.of("$gt", nearest)), null, null, 0, 0).size(),
                    "#379: a stored 2^53+1 is greater than the double 2^53 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("lng", Doc.of("$lte", nearest)), null, null, 0, 0).size(),
                    "#379: a stored 2^53+1 is NOT <= the double 2^53 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("lng", Doc.of("$eq", nearest)), null, null, 0, 0).size(),
                    "#344 rule holds in the operator path: no cross-type equality for 2^53+1 / 2^53.0");
            // stored Double 2^53 vs. query Long 2^53+1
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$lt", beyond)), null, null, 0, 0).size(),
                    "#379: a stored double 2^53 is less than the long 2^53+1 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$gte", beyond)), null, null, 0, 0).size(),
                    "#379: a stored double 2^53 is NOT >= the long 2^53+1 (compiled path)");
            // stored Long.MAX_VALUE vs. query Double 2^63
            assertEquals(1, drv.find(DB, COLL, Doc.of("max", Doc.of("$lt", twoPow63)), null, null, 0, 0).size(),
                    "#379: Long.MAX_VALUE is less than the double 2^63 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("max", Doc.of("$gte", twoPow63)), null, null, 0, 0).size(),
                    "#379: Long.MAX_VALUE is NOT >= the double 2^63 (compiled path)");
        } finally {
            drv.close();
        }

        assertTrue(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$gt", nearest)), Doc.of("lng", beyond), null),
                "#379: interpreted matcher, stored 2^53+1 > double 2^53");
        assertFalse(QueryHelper.matchesQuery(Doc.of("lng", Doc.of("$lte", nearest)), Doc.of("lng", beyond), null),
                "#379: interpreted matcher, stored 2^53+1 is not <= double 2^53");
        assertTrue(QueryHelper.matchesQuery(Doc.of("dbl", Doc.of("$lt", beyond)), Doc.of("dbl", nearest), null),
                "#379: interpreted matcher, stored double 2^53 < long 2^53+1");
        assertTrue(QueryHelper.matchesQuery(Doc.of("max", Doc.of("$lt", twoPow63)), Doc.of("max", Long.MAX_VALUE), null),
                "#379: interpreted matcher, Long.MAX_VALUE < double 2^63");
        assertFalse(QueryHelper.matchesQuery(Doc.of("max", Doc.of("$gte", twoPow63)), Doc.of("max", Long.MAX_VALUE), null),
                "#379: interpreted matcher, Long.MAX_VALUE is not >= double 2^63");
    }

    /**
     * -0.0 against the integer literal 0: Double.compare orders -0.0 below 0.0, so today
     * {$lt: 0} matched a stored -0.0 while {$eq: 0} did not. Under the exactness rule -0.0 IS
     * the exact long 0 (#344's isExactLong says so, and the direct path already matched), so
     * the operator path must agree: $eq/$lte/$gte hit, $lt/$gt/$ne miss - as in MongoDB.
     */
    @Test
    public void negativeZeroIsTheExactLongZeroInTheOperatorPath() throws Exception {
        InMemoryDriver drv = freshDriver();
        try {
            new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("dbl", -0.0d))).execute();

            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", 0), null, null, 0, 0).size(),
                    "#344 premise: the direct path already matches -0.0 with the literal 0");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$eq", 0)), null, null, 0, 0).size(),
                    "#379: {$eq: 0} must match a stored -0.0 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$lt", 0)), null, null, 0, 0).size(),
                    "#379: {$lt: 0} must NOT match a stored -0.0 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$gt", 0)), null, null, 0, 0).size(),
                    "#379: {$gt: 0} must NOT match a stored -0.0 (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$lte", 0)), null, null, 0, 0).size(),
                    "#379: {$lte: 0} must match a stored -0.0 (compiled path)");
            assertEquals(1, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$gte", 0)), null, null, 0, 0).size(),
                    "#379: {$gte: 0} must match a stored -0.0 (compiled path)");
            assertEquals(0, drv.find(DB, COLL, Doc.of("dbl", Doc.of("$ne", 0)), null, null, 0, 0).size(),
                    "#379: {$ne: 0} must NOT match a stored -0.0 (compiled path)");
        } finally {
            drv.close();
        }

        assertTrue(QueryHelper.matchesQuery(Doc.of("dbl", Doc.of("$eq", 0)), Doc.of("dbl", -0.0d), null),
                "#379: interpreted {$eq: 0} must match a stored -0.0");
        assertFalse(QueryHelper.matchesQuery(Doc.of("dbl", Doc.of("$lt", 0)), Doc.of("dbl", -0.0d), null),
                "#379: interpreted {$lt: 0} must NOT match a stored -0.0");
        assertTrue(QueryHelper.matchesQuery(Doc.of("dbl", Doc.of("$gte", 0L)), Doc.of("dbl", -0.0d), null),
                "#379: interpreted {$gte: 0L} must match a stored -0.0");
    }

    /**
     * Regression guard for the cascade: for |v| <= 2^53 every long is exactly a double, so the
     * old doubleValue() comparison was already right there and the new one must not move a
     * single answer. Mixed wrappers, non-integral doubles on both sides of an integer, negative
     * values, the boundary value 2^53 itself, and the Number types the cascade does NOT take
     * over (Float, BigDecimal - #343) all keep their behaviour.
     */
    @Test
    public void everydayValuesAreUnchangedByTheExactCascade() {
        // integral wrappers vs. each other
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", 3)), Doc.of("v", 2L), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gte", (short) 2)), Doc.of("v", 2), null));
        assertFalse(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gt", 2L)), Doc.of("v", (byte) 2), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", 2)), Doc.of("v", 2L), null));
        // integral vs. non-integral double, both directions, both signs
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gt", 2.5d)), Doc.of("v", 3L), null));
        assertFalse(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", 2.5d)), Doc.of("v", 3L), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", 3L)), Doc.of("v", 2.5d), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gt", -3.5d)), Doc.of("v", -3L), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", -2.5d)), Doc.of("v", -3L), null));
        assertFalse(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gt", -2.5d)), Doc.of("v", -3L), null));
        assertFalse(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", 2.5d)), Doc.of("v", 2), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$ne", 2.5d)), Doc.of("v", 2), null));
        // integral vs. exactly-integral double
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", 2.0d)), Doc.of("v", 2L), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lte", 2.0d)), Doc.of("v", 2), null));
        assertFalse(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", 2.0d)), Doc.of("v", 2), null));
        // the boundary itself: 2^53 as long and as double are the same value
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", (double) TWO_POW_53)), Doc.of("v", TWO_POW_53), null));
        assertFalse(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gt", (double) TWO_POW_53)), Doc.of("v", TWO_POW_53), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gte", TWO_POW_53)), Doc.of("v", (double) TWO_POW_53), null));
        // double vs. double keeps Double.compare (NaN equals NaN, infinities order)
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", 2.5d)), Doc.of("v", 2.25d), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", Double.NaN)), Doc.of("v", Double.NaN), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", Double.POSITIVE_INFINITY)), Doc.of("v", Long.MAX_VALUE), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$gt", Double.NEGATIVE_INFINITY)), Doc.of("v", Long.MIN_VALUE), null));
        // Number types outside the cascade stay on the old doubleValue() comparison (#343 for BigDecimal)
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", 2.5f)), Doc.of("v", 2.5d), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$lt", new java.math.BigDecimal("2.75"))), Doc.of("v", 2L), null));
        assertTrue(QueryHelper.matchesQuery(Doc.of("v", Doc.of("$eq", new java.math.BigDecimal("2"))), Doc.of("v", 2L), null));
    }

    /**
     * Structural half of #379: the TreeMap comparator behind every index range scan. Adjacent
     * longs past 2^53 must order strictly (today they compare EQUAL via doubleValue(), so the
     * TreeMap files them in one bucket and a bound built from one of them cuts the other off),
     * and a Long must order exactly against a Double that is not an exact long (2^63 is the
     * only such double near Long.MAX_VALUE, exact doubles are lifted to Long by IndexKey).
     */
    @Test
    public void indexKeyComparatorOrdersLongsExactlyBeyondDoublePrecision() {
        IndexDefinition def = IndexDefinition.fromIndexMap(Doc.of("lng", 1));
        Comparator<IndexKey> cmp = IndexKey.comparator(def);
        long p = TWO_POW_53;

        assertTrue(cmp.compare(IndexKey.of(List.of(p)), IndexKey.of(List.of(p + 1))) < 0,
                "#379: 2^53 must sort strictly below 2^53+1 in the index");
        assertTrue(cmp.compare(IndexKey.of(List.of(p + 1)), IndexKey.of(List.of(p + 2))) < 0,
                "#379: 2^53+1 must sort strictly below 2^53+2 in the index");
        assertTrue(cmp.compare(IndexKey.of(List.of(Long.MAX_VALUE)), IndexKey.of(List.of(0x1p63))) < 0,
                "#379: Long.MAX_VALUE must sort strictly below the double 2^63 in the index");
        assertTrue(cmp.compare(IndexKey.of(List.of(0x1p63)), IndexKey.of(List.of(Long.MAX_VALUE))) > 0,
                "#379: ...and the comparator must be antisymmetric about it");
        assertEquals(0, cmp.compare(IndexKey.of(List.of(p)), IndexKey.of(List.of((double) p))),
                "sanity: 2^53 as long and as double are one key");
        assertTrue(cmp.compare(IndexKey.of(List.of(2L)), IndexKey.of(List.of(2.5d))) < 0,
                "sanity: everyday long-vs-double ordering is unchanged");
    }

    /**
     * The test that makes IndexKey a mandatory part of the fix: with an index on the field, a
     * range query is answered by a TreeMap range scan whose bounds are IndexKeys; without one,
     * by the matcher over every document. Past 2^53 both used doubleValue() and were wrong in
     * DIFFERENT ways, so fixing only the matcher would make an indexed collection answer
     * differently from an unindexed one. Every operator/operand pair is checked against the
     * exact long arithmetic AND across the two drivers, and the counters prove the indexed
     * driver really served the query from the index.
     */
    @Test
    public void indexRangeScanAgreesWithCollscanBeyondDoublePrecision() throws Exception {
        long p = TWO_POW_53;
        List<Long> values = List.of(p - 1, p, p + 1, p + 2, p + 3, Long.MAX_VALUE - 1, Long.MAX_VALUE);
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long v : values) {
            docs.add(Doc.of("lng", v));
        }

        InMemoryDriver indexed = freshDriver();
        InMemoryDriver scanned = freshDriver();
        try {
            new InsertMongoCommand(indexed).setDb(DB).setColl(COLL).setDocuments(docs).execute();
            new InsertMongoCommand(scanned).setDb(DB).setColl(COLL).setDocuments(docs).execute();
            indexed.createIndex(DB, COLL, Doc.of("lng", 1), Doc.of("name", "lng_idx"));

            List<Long> operands = List.of(p, p + 1, p + 2, Long.MAX_VALUE - 1, Long.MAX_VALUE);
            for (long operand : operands) {
                for (String op : List.of("$lt", "$lte", "$gt", "$gte", "$eq")) {
                    Map<String, Object> query = Doc.of("lng", Doc.of(op, operand));
                    Set<Long> expected = new TreeSet<>();
                    for (long v : values) {
                        boolean hit = switch (op) {
                            case "$lt" -> v < operand;
                            case "$lte" -> v <= operand;
                            case "$gt" -> v > operand;
                            case "$gte" -> v >= operand;
                            default -> v == operand;
                        };
                        if (hit) {
                            expected.add(v);
                        }
                    }

                    long fullScansBefore = indexed.fullScans;
                    long indexHitsBefore = indexed.indexHits;
                    Set<Long> viaIndex = longs(indexed.find(DB, COLL, query, null, null, 0, 0));
                    if (!"$eq".equals(op)) {
                        // The planner turns the four range operators into a TreeMap RangeScan; the
                        // operator spelling of $eq is not planned (only the direct {lng: v} form is)
                        // and is compared here purely for matcher agreement.
                        assertEquals(indexHitsBefore + 1, indexed.indexHits, "test premise: " + query + " must be served by the index");
                        assertEquals(fullScansBefore, indexed.fullScans, "test premise: " + query + " must not fall back to a full scan");
                    }
                    Set<Long> viaScan = longs(scanned.find(DB, COLL, query, null, null, 0, 0));

                    assertEquals(expected, viaScan, "#379: collscan must be exact for " + query);
                    assertEquals(expected, viaIndex, "#379: index range scan must be exact for " + query);
                }
            }

            // A Double operand that is not an exact long: 2^63. Its IndexKey stays a Double, so the
            // TreeMap comparator has to order it against the stored longs exactly as the matcher does.
            Map<String, Object> belowTwoPow63 = Doc.of("lng", Doc.of("$lt", 0x1p63));
            assertEquals(new TreeSet<>(values), longs(scanned.find(DB, COLL, belowTwoPow63, null, null, 0, 0)),
                    "#379: every long is below the double 2^63 (collscan)");
            assertEquals(new TreeSet<>(values), longs(indexed.find(DB, COLL, belowTwoPow63, null, null, 0, 0)),
                    "#379: every long is below the double 2^63 (index range scan)");
        } finally {
            indexed.close();
            scanned.close();
        }
    }

    private static Set<Long> longs(List<Map<String, Object>> rows) {
        Set<Long> out = new TreeSet<>();
        for (Map<String, Object> row : rows) {
            out.add(((Number) row.get("lng")).longValue());
        }
        return out;
    }
}
