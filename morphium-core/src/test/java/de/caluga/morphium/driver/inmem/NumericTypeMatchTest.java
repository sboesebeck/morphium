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
}
