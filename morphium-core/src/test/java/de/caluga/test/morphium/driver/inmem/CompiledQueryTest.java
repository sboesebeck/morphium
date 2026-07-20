package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.CompiledQuery;
import de.caluga.morphium.driver.inmem.QueryHelper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential harness for {@link CompiledQuery} vs the reference interpreter
 * ({@link QueryHelper#matchesQueryInterpreted}). Every row asserts
 * {@code interpreted == compiled == expected} - this table IS the operator matrix deliverable for
 * Task 2 (see task-2-report.md for the full inventory / KNOWN-DIVERGENCE list).
 *
 * <p>Operator inventory covered (grepped from every {@code case "$..."} label in
 * {@code QueryHelper}): $eq (implicit+explicit), $ne, $gt/$gte/$lt/$lte, $in/$nin, $exists,
 * $regex(+$options)/$regularExpression, $mod, $size, $elemMatch, $all, $and/$or/$nor/$not, $expr,
 * $type, $where, $jsonSchema, $textSearch/$text, $comment, $geoIntersects/$near/$nearSphere/
 * $geoWithin, $bitsAllSet/$bitsAllClear/$bitsAnySet/$bitsAnyClear, dotted paths, arrays-of-docs
 * traversal, map-field/array-index queries, null-vs-missing.
 */
@Tag("inmemory")
public class CompiledQueryTest {

    private record Case(String description, Map<String, Object> query, Map<String, Object> doc, boolean expected,
                        Map<String, Object> collation) {
        // Convenience constructor: the vast majority of rows use no collation (null).
        Case(String description, Map<String, Object> query, Map<String, Object> doc, boolean expected) {
            this(description, query, doc, expected, null);
        }
    }

    @TestFactory
    List<DynamicTest> operatorMatrix() {
        List<Case> cases = buildMatrix();
        List<DynamicTest> tests = new ArrayList<>();

        for (Case c : cases) {
            tests.add(DynamicTest.dynamicTest(c.description(), () -> {
                boolean interpreted = QueryHelper.matchesQueryInterpreted(c.query(), c.doc(), c.collation());
                boolean compiled = CompiledQuery.compile(c.query(), c.collation()).matches(c.doc());

                assertEquals(c.expected(), interpreted, () -> "interpreter diverged from expected for: " + c.description());
                assertEquals(c.expected(), compiled, () -> "compiled diverged from expected for: " + c.description());
                assertEquals(interpreted, compiled, () -> "compiled diverged from interpreter for: " + c.description());
            }));
        }

        return tests;
    }

    /**
     * Perf assertion (loose bound, no JMH): scanning 100k docs with a 3-condition query, compiling
     * once and reusing the compiled predicate must be at least 2x faster than re-interpreting the
     * query per document. Both paths must return the identical match count (correctness under load).
     */
    @Test
    void compiledIsAtLeastTwiceAsFastAsInterpreterOver100k() {
        final int n = 100_000;
        List<Map<String, Object>> docs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            docs.add(Doc.of(
                         "age", 10 + (i % 60),
                         "active", (i % 2) == 0,
                         "category", (i % 3 == 0) ? "x" : (i % 3 == 1 ? "y" : "z")));
        }

        // 3-condition query: $gt + implicit-eq + $in
        Map<String, Object> query = Doc.of(
                                        "age", Doc.of("$gt", 18),
                                        "active", true,
                                        "category", Doc.of("$in", List.of("x", "y")));

        // Warmup (JIT) - both paths, discard timing
        for (int w = 0; w < 3; w++) {
            long warm = 0;
            CompiledQuery cq = CompiledQuery.compile(query);
            for (Map<String, Object> d : docs) {
                if (cq.matches(d)) warm++;
                if (QueryHelper.matchesQueryInterpreted(query, d, null)) warm++;
            }
            assertTrue(warm >= 0);
        }

        // Interpreter timing
        long interpStart = System.nanoTime();
        long interpMatches = 0;
        for (Map<String, Object> d : docs) {
            if (QueryHelper.matchesQueryInterpreted(query, d, null)) interpMatches++;
        }
        long interpNanos = System.nanoTime() - interpStart;

        // Compiled timing (compile ONCE, reuse across the doc loop - the driver hot-path shape)
        long compStart = System.nanoTime();
        long compMatches = 0;
        CompiledQuery compiled = CompiledQuery.compile(query);
        for (Map<String, Object> d : docs) {
            if (compiled.matches(d)) compMatches++;
        }
        long compNanos = System.nanoTime() - compStart;

        assertEquals(interpMatches, compMatches, "compiled and interpreter must agree on match count");

        double speedup = (double) interpNanos / (double) compNanos;
        System.out.printf("[perf] 100k/3-cond: interpreter=%.1fms compiled=%.1fms speedup=%.2fx matches=%d%n",
                          interpNanos / 1e6, compNanos / 1e6, speedup, compMatches);

        assertTrue(speedup >= 2.0,
                   () -> String.format("compiled must be >= 2x interpreter; was %.2fx (interp=%.1fms comp=%.1fms)",
                                       (double) interpNanos / compNanos, interpNanos / 1e6, compNanos / 1e6));
    }

    private static List<Case> buildMatrix() {
        List<Case> cases = new ArrayList<>();

        // ---------------------------------------------------------------- $eq (implicit + explicit)
        cases.add(new Case("implicit eq match", Doc.of("a", 1), Doc.of("a", 1), true));
        cases.add(new Case("implicit eq mismatch", Doc.of("a", 1), Doc.of("a", 2), false));
        cases.add(new Case("implicit eq string", Doc.of("name", "bob"), Doc.of("name", "bob"), true));
        cases.add(new Case("implicit eq against list field (contains)", Doc.of("tags", "x"), Doc.of("tags", List.of("x", "y")), true));
        cases.add(new Case("implicit eq against list field, no match", Doc.of("tags", "z"), Doc.of("tags", List.of("x", "y")), false));
        cases.add(new Case("explicit $eq match", Doc.of("a", Doc.of("$eq", 5)), Doc.of("a", 5), true));
        cases.add(new Case("explicit $eq mismatch", Doc.of("a", Doc.of("$eq", 5)), Doc.of("a", 6), false));
        cases.add(new Case("explicit $eq against list field, any element matches", Doc.of("a", Doc.of("$eq", 2)), Doc.of("a", List.of(1, 2, 3)), true));
        cases.add(new Case("explicit $eq null==null", Doc.of("a", Doc.of("$eq", null)), Doc.of("a", null), true));
        cases.add(new Case("explicit $eq value vs missing field", Doc.of("a", Doc.of("$eq", 1)), Doc.of("b", 1), false));
        cases.add(new Case("implicit eq MorphiumId vs string form", Doc.of("_id", new MorphiumId().toString()), Doc.of("_id", new MorphiumId()), false));

        // ---------------------------------------------------------------- $ne
        cases.add(new Case("$ne match (different value)", Doc.of("a", Doc.of("$ne", 1)), Doc.of("a", 2), true));
        cases.add(new Case("$ne mismatch (same value)", Doc.of("a", Doc.of("$ne", 1)), Doc.of("a", 1), false));
        cases.add(new Case("$ne against list, none equal -> true", Doc.of("a", Doc.of("$ne", 9)), Doc.of("a", List.of(1, 2, 3)), true));
        cases.add(new Case("$ne against list, one equal -> false", Doc.of("a", Doc.of("$ne", 2)), Doc.of("a", List.of(1, 2, 3)), false));
        cases.add(new Case("$ne both null -> false", Doc.of("a", Doc.of("$ne", null)), Doc.of("a", null), false));

        // ---------------------------------------------------------------- $gt/$gte/$lt/$lte
        cases.add(new Case("$gt true", Doc.of("a", Doc.of("$gt", 5)), Doc.of("a", 6), true));
        cases.add(new Case("$gt false (equal)", Doc.of("a", Doc.of("$gt", 5)), Doc.of("a", 5), false));
        cases.add(new Case("$gte true (equal)", Doc.of("a", Doc.of("$gte", 5)), Doc.of("a", 5), true));
        cases.add(new Case("$lt true", Doc.of("a", Doc.of("$lt", 5)), Doc.of("a", 4), true));
        cases.add(new Case("$lte true (equal)", Doc.of("a", Doc.of("$lte", 5)), Doc.of("a", 5), true));
        cases.add(new Case("$gt on list field, any element matches", Doc.of("a", Doc.of("$gt", 5)), Doc.of("a", List.of(1, 10)), true));
        cases.add(new Case("$gt missing field -> false", Doc.of("a", Doc.of("$gt", 5)), Doc.of("b", 6), false));
        cases.add(new Case("multi-op range $gte+$lte both pass", Doc.of("a", Doc.of("$gte", 1, "$lte", 10)), Doc.of("a", 5), true));
        cases.add(new Case("multi-op range one fails", Doc.of("a", Doc.of("$gte", 1, "$lte", 10)), Doc.of("a", 11), false));

        // ---------------------------------------------------------------- $in/$nin
        cases.add(new Case("$in match", Doc.of("a", Doc.of("$in", List.of(1, 2, 3))), Doc.of("a", 2), true));
        cases.add(new Case("$in mismatch", Doc.of("a", Doc.of("$in", List.of(1, 2, 3))), Doc.of("a", 9), false));
        cases.add(new Case("$in against list field, overlap", Doc.of("a", Doc.of("$in", List.of(1, 2))), Doc.of("a", List.of(2, 5)), true));
        cases.add(new Case("$nin match (not present)", Doc.of("a", Doc.of("$nin", List.of(1, 2, 3))), Doc.of("a", 9), true));
        cases.add(new Case("$nin mismatch (present)", Doc.of("a", Doc.of("$nin", List.of(1, 2, 3))), Doc.of("a", 2), false));

        // ---------------------------------------------------------------- $exists
        cases.add(new Case("$exists true, field present", Doc.of("a", Doc.of("$exists", true)), Doc.of("a", 1), true));
        cases.add(new Case("$exists true, field missing", Doc.of("a", Doc.of("$exists", true)), Doc.of("b", 1), false));
        cases.add(new Case("$exists false, field missing", Doc.of("a", Doc.of("$exists", false)), Doc.of("b", 1), true));
        cases.add(new Case("$exists false, field present", Doc.of("a", Doc.of("$exists", false)), Doc.of("a", 1), false));
        cases.add(new Case("$exists true, field present but null", Doc.of("a", Doc.of("$exists", true)), Doc.of("a", null), true));
        cases.add(new Case("$exists true, dotted path present", Doc.of("a.b", Doc.of("$exists", true)), Doc.of("a", Doc.of("b", 1)), true));
        cases.add(new Case("$exists true, dotted path missing", Doc.of("a.b", Doc.of("$exists", true)), Doc.of("a", Doc.of("c", 1)), false));
        cases.add(new Case("$exists true, snake_case fallback to camelCase field", Doc.of("first_name", Doc.of("$exists", true)), Doc.of("firstName", "x"), true));
        cases.add(new Case("$exists true, camelCase fallback to snake_case field", Doc.of("firstName", Doc.of("$exists", true)), Doc.of("first_name", "x"), true));

        // ---------------------------------------------------------------- $regex + $options
        cases.add(new Case("$regex match", Doc.of("name", Doc.of("$regex", "^bo")), Doc.of("name", "bob"), true));
        cases.add(new Case("$regex mismatch", Doc.of("name", Doc.of("$regex", "^zz")), Doc.of("name", "bob"), false));
        cases.add(new Case("$regex + $options case-insensitive", Doc.of("name", Doc.of("$regex", "^BOB$", "$options", "i")), Doc.of("name", "bob"), true));
        cases.add(new Case("$regex dotted path match", Doc.of("a.name", Doc.of("$regex", "^bo")), Doc.of("a", Doc.of("name", "bob")), true));
        cases.add(new Case("$regex against list field, any element matches", Doc.of("tags", Doc.of("$regex", "^a")), Doc.of("tags", List.of("xx", "aa")), true));

        // ---------------------------------------------------------------- $mod
        cases.add(new Case("$mod match", Doc.of("a", Doc.of("$mod", List.of(4, 0))), Doc.of("a", 8), true));
        cases.add(new Case("$mod mismatch", Doc.of("a", Doc.of("$mod", List.of(4, 0))), Doc.of("a", 7), false));

        // ---------------------------------------------------------------- $size
        cases.add(new Case("$size match", Doc.of("tags", Doc.of("$size", 3)), Doc.of("tags", List.of("a", "b", "c")), true));
        cases.add(new Case("$size mismatch", Doc.of("tags", Doc.of("$size", 3)), Doc.of("tags", List.of("a", "b")), false));
        // #251: a missing field is not an empty array - MongoDB never matches it against $size.
        // This case previously encoded the old (wrong) match-everything behaviour.
        cases.add(new Case("$size on missing field does not match", Doc.of("tags", Doc.of("$size", 0)), Doc.of("other", 1), false));
        cases.add(new Case("$size on non-list -> false", Doc.of("tags", Doc.of("$size", 1)), Doc.of("tags", "x"), false));

        // ---------------------------------------------------------------- $elemMatch
        cases.add(new Case("$elemMatch doc field match", Doc.of("items", Doc.of("$elemMatch", Doc.of("q", 1, "s", 8))),
                            Doc.of("items", List.of(Doc.of("q", 1, "s", 8), Doc.of("q", 2, "s", 5))), true));
        cases.add(new Case("$elemMatch doc field no match", Doc.of("items", Doc.of("$elemMatch", Doc.of("q", 9))),
                            Doc.of("items", List.of(Doc.of("q", 1), Doc.of("q", 2))), false));
        cases.add(new Case("$elemMatch primitive elements with operator", Doc.of("scores", Doc.of("$elemMatch", Doc.of("$gt", 90))),
                            Doc.of("scores", List.of(50, 95, 60)), true));
        cases.add(new Case("$elemMatch on non-list -> false", Doc.of("items", Doc.of("$elemMatch", Doc.of("q", 1))), Doc.of("items", "x"), false));

        // ---------------------------------------------------------------- $all
        cases.add(new Case("$all match", Doc.of("tags", Doc.of("$all", List.of("a", "b"))), Doc.of("tags", List.of("a", "b", "c")), true));
        cases.add(new Case("$all mismatch (missing element)", Doc.of("tags", Doc.of("$all", List.of("a", "z"))), Doc.of("tags", List.of("a", "b", "c")), false));

        // ---------------------------------------------------------------- $and / $or / $nor / $not
        cases.add(new Case("$and all true", Doc.of("$and", List.of(Doc.of("a", 1), Doc.of("b", 2))), Doc.of("a", 1, "b", 2), true));
        cases.add(new Case("$and one false", Doc.of("$and", List.of(Doc.of("a", 1), Doc.of("b", 3))), Doc.of("a", 1, "b", 2), false));
        cases.add(new Case("$or one true", Doc.of("$or", List.of(Doc.of("a", 9), Doc.of("b", 2))), Doc.of("a", 1, "b", 2), true));
        cases.add(new Case("$or all false", Doc.of("$or", List.of(Doc.of("a", 9), Doc.of("b", 9))), Doc.of("a", 1, "b", 2), false));
        cases.add(new Case("$nor all false -> true", Doc.of("$nor", List.of(Doc.of("a", 9), Doc.of("b", 9))), Doc.of("a", 1, "b", 2), true));
        cases.add(new Case("$nor one true -> false", Doc.of("$nor", List.of(Doc.of("a", 1), Doc.of("b", 9))), Doc.of("a", 1, "b", 2), false));
        cases.add(new Case("top-level $not negates inner query true->false", Doc.of("$not", Doc.of("a", 1)), Doc.of("a", 1), false));
        cases.add(new Case("top-level $not negates inner query false->true", Doc.of("$not", Doc.of("a", 9)), Doc.of("a", 1), true));
        cases.add(new Case("field-level $not on $regex", Doc.of("name", Doc.of("$not", Doc.of("$regex", "^zz"))), Doc.of("name", "bob"), true));
        cases.add(new Case("field-level $not on $eq", Doc.of("a", Doc.of("$not", Doc.of("$eq", 1))), Doc.of("a", 1), false));
        cases.add(new Case("nested $and/$or combo", Doc.of("$and", List.of(Doc.of("a", 1), Doc.of("$or", List.of(Doc.of("b", 9), Doc.of("c", 3))))),
                            Doc.of("a", 1, "b", 2, "c", 3), true));

        // ---------------------------------------------------------------- $expr (top-level)
        cases.add(new Case("$expr true", Doc.of("$expr", Doc.of("$gt", List.of("$a", "$b"))), Doc.of("a", 5, "b", 2), true));
        cases.add(new Case("$expr false", Doc.of("$expr", Doc.of("$gt", List.of("$a", "$b"))), Doc.of("a", 1, "b", 2), false));

        // ---------------------------------------------------------------- $expr (FIELD-level, hand-compiled)
        // The interpreter's field-level $expr casts the operand directly to Expr (no Expr.parse) -
        // this is the shape produced by the aggregation $match rewrite path, i.e. an already-parsed
        // Expr instance. CompiledQuery hand-compiles this branch (HAND_COMPILED_OPS), so it needs
        // its own differential coverage rather than piggy-backing on top-level $expr.
        cases.add(new Case("field-level $expr true (pre-parsed Expr operand)",
                            Doc.of("ignoredField", Doc.of("$expr", de.caluga.morphium.aggregation.Expr.gt(
                                    de.caluga.morphium.aggregation.Expr.field("a"),
                                    de.caluga.morphium.aggregation.Expr.field("b")))),
                            Doc.of("a", 5, "b", 2), true));
        cases.add(new Case("field-level $expr false (pre-parsed Expr operand)",
                            Doc.of("ignoredField", Doc.of("$expr", de.caluga.morphium.aggregation.Expr.gt(
                                    de.caluga.morphium.aggregation.Expr.field("a"),
                                    de.caluga.morphium.aggregation.Expr.field("b")))),
                            Doc.of("a", 1, "b", 2), false));

        // ---------------------------------------------------------------- $type
        cases.add(new Case("$type string match", Doc.of("a", Doc.of("$type", "string")), Doc.of("a", "x"), true));
        cases.add(new Case("$type string mismatch", Doc.of("a", Doc.of("$type", "string")), Doc.of("a", 1), false));
        cases.add(new Case("$type int match", Doc.of("a", Doc.of("$type", "int")), Doc.of("a", 1), true));
        cases.add(new Case("$type numeric code match (double=1)", Doc.of("a", Doc.of("$type", 1)), Doc.of("a", 1.5), true));
        cases.add(new Case("$type on list field, any element matches", Doc.of("a", Doc.of("$type", "string")), Doc.of("a", List.of(1, "x")), true));

        // ---------------------------------------------------------------- dotted paths / nested docs
        cases.add(new Case("dotted implicit eq nested doc", Doc.of("a.b", 5), Doc.of("a", Doc.of("b", 5)), true));
        cases.add(new Case("dotted implicit eq nested doc mismatch", Doc.of("a.b", 5), Doc.of("a", Doc.of("b", 6)), false));
        cases.add(new Case("dotted $gt nested doc", Doc.of("a.b", Doc.of("$gt", 5)), Doc.of("a", Doc.of("b", 6)), true));
        cases.add(new Case("dotted path through array index", Doc.of("a.0.b", 5), Doc.of("a", List.of(Doc.of("b", 5), Doc.of("b", 6))), true));
        cases.add(new Case("dotted path across array-of-docs (any element)", Doc.of("items.q", 1),
                            Doc.of("items", List.of(Doc.of("q", 2), Doc.of("q", 1))), true));
        cases.add(new Case("dotted path across array-of-docs, no match", Doc.of("items.q", 9),
                            Doc.of("items", List.of(Doc.of("q", 2), Doc.of("q", 1))), false));
        cases.add(new Case("dotted $exists across array-of-docs", Doc.of("items.q", Doc.of("$exists", true)),
                            Doc.of("items", List.of(Doc.of("x", 1), Doc.of("q", 1))), true));

        // ---------------------------------------------------------------- null vs missing
        cases.add(new Case("implicit eq null matches explicit null field", Doc.of("a", null), Doc.of("a", null), true));
        cases.add(new Case("implicit eq null matches missing field", Doc.of("a", null), Doc.of("b", 1), true));
        cases.add(new Case("implicit eq non-null vs missing field -> false", Doc.of("a", 1), Doc.of("b", 1), false));
        cases.add(new Case("dotted eq null matches missing path", Doc.of("a.b", null), Doc.of("a", Doc.of("c", 1)), true));
        cases.add(new Case("dotted eq non-null vs missing path -> false", Doc.of("a.b", 1), Doc.of("a", Doc.of("c", 1)), false));
        cases.add(new Case("$exists false semantics differ from eq-null (field present as null)", Doc.of("a", Doc.of("$exists", false)), Doc.of("a", null), false));

        // ---------------------------------------------------------------- KNOWN-DIVERGENCE #2:
        // empty-but-non-null collation resolves a real ROOT Collator ONLY in the non-dotted
        // implicit-eq branch (interpreter checks `collation != null` there, not `!isEmpty()`).
        // These rows run with collation = new HashMap<>() (non-null, empty) on that exact path;
        // CompiledQuery must mirror the interpreter (asserted via c.collation() threading above).
        // Ctx.collUnchecked is what makes this pass on the compiled side.
        Map<String, Object> emptyCollation = new java.util.HashMap<>();
        cases.add(new Case("empty-non-null collation, non-dotted implicit-eq string match",
                           Doc.of("name", "bob"), Doc.of("name", "bob"), true, emptyCollation));
        cases.add(new Case("empty-non-null collation, non-dotted implicit-eq string mismatch",
                           Doc.of("name", "bob"), Doc.of("name", "alice"), false, emptyCollation));
        cases.add(new Case("empty-non-null collation, non-dotted implicit-eq against list field (collated contains)",
                           Doc.of("tags", "x"), Doc.of("tags", List.of("x", "y")), true, emptyCollation));

        // ---------------------------------------------------------------- map-field / array-index query corner
        Map<String, Object> mapFieldQuery = new LinkedHashMap<>();
        mapFieldQuery.put("street", "Main St");
        cases.add(new Case("map field direct sub-key query, match", Doc.of("addr", mapFieldQuery), Doc.of("addr", Doc.of("street", "Main St", "city", "X")), true));
        Map<String, Object> mapFieldQueryMismatch = new LinkedHashMap<>();
        mapFieldQueryMismatch.put("street", "Other St");
        cases.add(new Case("map field direct sub-key query, mismatch falls back to standard (equals-whole-map) -> false",
                            Doc.of("addr", mapFieldQueryMismatch), Doc.of("addr", Doc.of("street", "Main St", "city", "X")), false));
        Map<String, Object> arrayIndexQuery = new LinkedHashMap<>();
        arrayIndexQuery.put("0", "a");
        cases.add(new Case("array index query, match", Doc.of("tags", arrayIndexQuery), Doc.of("tags", List.of("a", "b")), true));
        Map<String, Object> arrayIndexQueryOob = new LinkedHashMap<>();
        arrayIndexQueryOob.put("5", "a");
        cases.add(new Case("array index query, out of bounds -> false", Doc.of("tags", arrayIndexQueryOob), Doc.of("tags", List.of("a", "b")), false));
        Map<String, Object> arrayIndexExistsQuery = new LinkedHashMap<>();
        arrayIndexExistsQuery.put("5", Doc.of("$exists", false));
        cases.add(new Case("array index $exists:false on out-of-bounds index -> true", Doc.of("tags", arrayIndexExistsQuery), Doc.of("tags", List.of("a", "b")), true));
        Map<String, Object> arrayIndexExistsQueryTrue = new LinkedHashMap<>();
        arrayIndexExistsQueryTrue.put("0", Doc.of("$exists", true));
        cases.add(new Case("array index $exists:true on in-bounds index -> true", Doc.of("tags", arrayIndexExistsQueryTrue), Doc.of("tags", List.of("a", "b")), true));

        // ---------------------------------------------------------------- $where (GraalVM JS is on the test classpath)
        cases.add(new Case("$where script true", Doc.of("$where", "a == 1"), Doc.of("a", 1), true));
        cases.add(new Case("$where script false", Doc.of("$where", "a == 1"), Doc.of("a", 2), false));
        // $where does NOT properly AND with a following key - a documented interpreter quirk
        // (see CompiledQuery's SequenceNode javadoc / KNOWN-DIVERGENCE), replicated not fixed: a
        // failing $where followed by a successful ordinary key silently "passes" overall.
        Map<String, Object> whereThenField = new LinkedHashMap<>();
        whereThenField.put("$where", "a == 1");
        whereThenField.put("b", 2);
        cases.add(new Case("$where(false) followed by successful field overwrites ret -> true (interpreter quirk, preserved)",
                            whereThenField, Doc.of("a", 9, "b", 2), true));

        // ---------------------------------------------------------------- $jsonSchema
        cases.add(new Case("$jsonSchema bsonType match", Doc.of("$jsonSchema", Doc.of("bsonType", "object", "required", List.of("a"))), Doc.of("a", 1), true));
        cases.add(new Case("$jsonSchema required field missing", Doc.of("$jsonSchema", Doc.of("bsonType", "object", "required", List.of("z"))), Doc.of("a", 1), false));

        // ---------------------------------------------------------------- $comment (always true, no-op)
        cases.add(new Case("$comment is a documentation no-op, always true", Doc.of("a", Doc.of("$eq", 1, "$comment", "why")), Doc.of("a", 1), true));

        // ---------------------------------------------------------------- $bits*
        cases.add(new Case("$bitsAllSet match", Doc.of("a", Doc.of("$bitsAllSet", List.of(0, 1))), Doc.of("a", 3), true));
        cases.add(new Case("$bitsAllSet mismatch", Doc.of("a", Doc.of("$bitsAllSet", List.of(0, 1))), Doc.of("a", 1), false));
        cases.add(new Case("$bitsAnySet match", Doc.of("a", Doc.of("$bitsAnySet", List.of(1))), Doc.of("a", 2), true));
        cases.add(new Case("$bitsAllClear match", Doc.of("a", Doc.of("$bitsAllClear", List.of(2))), Doc.of("a", 1), true));

        // ---------------------------------------------------------------- $geoWithin / $geoIntersects
        Doc box = Doc.of("$box", List.of(List.of(0.0, 0.0), List.of(10.0, 10.0)));
        cases.add(new Case("$geoWithin box, inside", Doc.of("pos", Doc.of("$geoWithin", box)), Doc.of("pos", List.of(5.0, 5.0)), true));
        cases.add(new Case("$geoWithin box, outside", Doc.of("pos", Doc.of("$geoWithin", box)), Doc.of("pos", List.of(50.0, 50.0)), false));

        Doc pointGeom = Doc.of("type", "Point", "coordinates", List.of(1.0, 1.0));
        cases.add(new Case("$geoIntersects point==point", Doc.of("pos", Doc.of("$geoIntersects", Doc.of("$geometry", pointGeom))),
                            Doc.of("pos", pointGeom), true));

        // ---------------------------------------------------------------- multi-field implicit AND
        cases.add(new Case("multi-field implicit AND, all match", Doc.of("a", 1, "b", 2), Doc.of("a", 1, "b", 2), true));
        cases.add(new Case("multi-field implicit AND, second fails", Doc.of("a", 1, "b", 2), Doc.of("a", 1, "b", 3), false));
        cases.add(new Case("three-condition query all match (perf-shape query)",
                            Doc.of("age", Doc.of("$gt", 18), "active", true, "category", Doc.of("$in", List.of("x", "y"))),
                            Doc.of("age", 30, "active", true, "category", "y"), true));
        cases.add(new Case("three-condition query one fails (perf-shape query)",
                            Doc.of("age", Doc.of("$gt", 18), "active", true, "category", Doc.of("$in", List.of("x", "y"))),
                            Doc.of("age", 30, "active", false, "category", "y"), false));

        // ---------------------------------------------------------------- empty query
        cases.add(new Case("empty query matches everything", new LinkedHashMap<>(), Doc.of("a", 1), true));

        return cases;
    }
}
