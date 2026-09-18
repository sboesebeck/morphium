package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression tests for #376: a {@code $group} accumulator whose operand is an aggregation
 * EXPRESSION (e.g. {@code {$sum: {$cond: [...]}}}) silently produced 0 / null / a raw spec
 * instead of evaluating the expression per document. The expression evaluator itself handles
 * these operands fine (it is what {@code $project} uses for computed fields), so the defect sat
 * in the accumulators' operand handling. Against mongod the very same pipelines compute
 * correctly.
 *
 * <p>Pipelines are given in their raw {@code Map} form (as they arrive over the wire resp.
 * via {@link AggregateMongoCommand}) - the shape the reporting application uses.
 */
@Tag("inmemory")
public class GroupAccumulatorExpressionTest {
    private static final String DB = "group_accu_expr_db";
    private static final String COLL = "stats";

    private InMemoryDriver drv;

    @BeforeEach
    public void setUp() throws Exception {
        drv = new InMemoryDriver();
        drv.setExpireCheck(3_600_000);
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        drv.close();
    }

    private void insert(List<Map<String, Object>> docs) throws Exception {
        new InsertMongoCommand(drv).setDb(DB).setColl(COLL).setDocuments(docs).execute();
    }

    /** counter 1..4; result is "OK" for 1 and 2, "FAIL" for 3 and 4. */
    private void insertFourDocs() throws Exception {
        insert(List.of(
                Doc.of("counter", 1, "result", "OK"),
                Doc.of("counter", 2, "result", "OK"),
                Doc.of("counter", 3, "result", "FAIL"),
                Doc.of("counter", 4, "result", "FAIL")));
    }

    private Map<String, Object> groupAll(Map<String, Object> accumulators) throws Exception {
        Map<String, Object> group = Doc.of("_id", null);
        group.putAll(accumulators);
        List<Map<String, Object>> res = new AggregateMongoCommand(drv).setDb(DB).setColl(COLL)
                .setPipeline(List.of(Doc.of("$sort", Doc.of("counter", 1)), Doc.of("$group", group)))
                .execute();
        assertEquals(1, res.size(), "grouping on _id:null must yield exactly one document");
        return res.get(0);
    }

    /** The issue's headline case: 101 documents, {$sum: {$cond: [true, 1, 0]}} next to {$sum: 1}. */
    @Test
    public void sumOverConstantTrueCondCountsEveryDocument() throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            docs.add(Doc.of("counter", i, "result", "OK"));
        }
        insert(docs);

        Map<String, Object> r = groupAll(Doc.of(
                "viaSumOne", Doc.of("$sum", 1),
                "viaSumExpr", Doc.of("$sum", Doc.of("$cond", List.of(true, 1, 0)))));

        assertEquals(101, ((Number) r.get("viaSumOne")).intValue());
        assertEquals(101, ((Number) r.get("viaSumExpr")).intValue(),
                "#376: $sum over a $cond expression must count like $sum: 1");
    }

    /** Both $cond spellings (array and {if,then,else}) with a per-document condition. */
    @Test
    public void sumOverCondExpressionCountsMatchingDocumentsOnly() throws Exception {
        insertFourDocs();
        Map<String, Object> isOk = Doc.of("$eq", List.of("$result", "OK"));

        Map<String, Object> r = groupAll(Doc.of(
                "okArrayForm", Doc.of("$sum", Doc.of("$cond", List.of(isOk, 1, 0))),
                "okObjectForm", Doc.of("$sum", Doc.of("$cond", Doc.of("if", isOk, "then", 1, "else", 0))),
                "doubled", Doc.of("$sum", Doc.of("$multiply", List.of("$counter", 2)))));

        assertEquals(2, ((Number) r.get("okArrayForm")).intValue(), "#376: array-form $cond");
        assertEquals(2, ((Number) r.get("okObjectForm")).intValue(), "#376: object-form $cond");
        assertEquals(20, ((Number) r.get("doubled")).intValue(), "#376: $sum over $multiply");
    }

    @Test
    public void avgMinMaxOverExpression() throws Exception {
        insertFourDocs();
        Map<String, Object> doubled = Doc.of("$multiply", List.of("$counter", 2));

        Map<String, Object> r = groupAll(Doc.of(
                "avg", Doc.of("$avg", doubled),
                "min", Doc.of("$min", doubled),
                "max", Doc.of("$max", doubled)));

        assertEquals(5.0, ((Number) r.get("avg")).doubleValue(), 0.0001, "#376: $avg over expression");
        assertEquals(2, ((Number) r.get("min")).intValue(), "#376: $min over expression");
        assertEquals(8, ((Number) r.get("max")).intValue(), "#376: $max over expression");
    }

    @Test
    public void firstLastOverExpression() throws Exception {
        insertFourDocs();
        Map<String, Object> tenfold = Doc.of("$multiply", List.of("$counter", 10));

        Map<String, Object> r = groupAll(Doc.of(
                "first", Doc.of("$first", tenfold),
                "last", Doc.of("$last", tenfold)));

        assertEquals(10, ((Number) r.get("first")).intValue(), "#376: $first over expression");
        assertEquals(40, ((Number) r.get("last")).intValue(), "#376: $last over expression");
    }

    @Test
    public void pushAndAddToSetOverExpression() throws Exception {
        insertFourDocs();
        Map<String, Object> size = Doc.of("$cond", List.of(Doc.of("$gt", List.of("$counter", 2)), "big", "small"));

        Map<String, Object> r = groupAll(Doc.of(
                "pushed", Doc.of("$push", size),
                "set", Doc.of("$addToSet", size)));

        assertEquals(List.of("small", "small", "big", "big"), r.get("pushed"), "#376: $push over expression");
        assertEquals(List.of("small", "big"), r.get("set"), "#376: $addToSet over expression");
    }

    /**
     * The "$field" fast path of $sum cast the value to Number unchecked: a document without
     * the field (or with a non-numeric value) killed the whole aggregation with an NPE /
     * ClassCastException, while the expression path next to it already ignored such values.
     * mongod ignores missing and non-numeric values in $sum, so the sum over the numeric
     * documents is the expected result.
     */
    @Test
    public void sumOverFieldReferenceIgnoresMissingAndNonNumericValues() throws Exception {
        insert(List.of(
                Doc.of("counter", 1, "amount", 10),
                Doc.of("counter", 2),
                Doc.of("counter", 3, "amount", "n/a"),
                Doc.of("counter", 4, "amount", 5.5)));

        Map<String, Object> r = groupAll(Doc.of("total", Doc.of("$sum", "$amount")));

        assertEquals(15.5, ((Number) r.get("total")).doubleValue(), 0.0001,
                "$sum over \"$field\" must skip missing and non-numeric values like mongod");
    }

    /**
     * The {if,then,else} spelling of $cond was not parseable at all (Expr.parse only knew the
     * positional form), so it must now work wherever the array form does - checked here on the
     * $project path (strict inclusion mode, which evaluates raw Map expressions).
     */
    @Test
    public void condObjectFormEvaluatesInProject() throws Exception {
        insertFourDocs();
        Map<String, Object> isOk = Doc.of("$eq", List.of("$result", "OK"));

        List<Map<String, Object>> res = new AggregateMongoCommand(drv).setDb(DB).setColl(COLL)
                .setPipeline(List.of(
                        Doc.of("$sort", Doc.of("counter", 1)),
                        Doc.of("$project", Doc.of(
                                "counter", 1,
                                "arrayForm", Doc.of("$cond", List.of(isOk, 1, 0)),
                                "objectForm", Doc.of("$cond", Doc.of("if", isOk, "then", 1, "else", 0))))))
                .execute();

        assertEquals(4, res.size());
        assertEquals(1, ((Number) res.get(0).get("arrayForm")).intValue());
        assertEquals(1, ((Number) res.get(0).get("objectForm")).intValue(), "object-form $cond in $project");
        assertEquals(0, ((Number) res.get(3).get("objectForm")).intValue(), "object-form $cond in $project");
    }

    /**
     * #377/#378: for mongod a $project consisting only of computed fields IS an inclusion
     * projection - the result carries _id plus the computed fields and nothing else. The
     * in-memory driver classified such a spec as lenient/exclusion mode, kept the whole document
     * and then, for a raw operator Map, iterated the spec's operator keys ($cond) as field names,
     * logged "only works with Expr" and never wrote the computed field. Adding any {field: 1}
     * next to it switched to strict mode and made the very same expression work.
     */
    @Test
    public void computedOnlyProjectIsAnInclusionProjection() throws Exception {
        insertFourDocs();
        Map<String, Object> isOk = Doc.of("$eq", List.of("$result", "OK"));

        List<Map<String, Object>> res = new AggregateMongoCommand(drv).setDb(DB).setColl(COLL)
                .setPipeline(List.of(
                        Doc.of("$sort", Doc.of("counter", 1)),
                        Doc.of("$project", Doc.of(
                                "arrayForm", Doc.of("$cond", List.of(isOk, 1, 0)),
                                "objectForm", Doc.of("$cond", Doc.of("if", isOk, "then", 1, "else", 0)),
                                "ref", "$counter"))))
                .execute();

        assertEquals(4, res.size());
        Map<String, Object> first = res.get(0);
        assertEquals(Set.of("_id", "arrayForm", "objectForm", "ref"), first.keySet(),
                "#378: a computed-only $project must behave like inclusion mode (_id + computed fields)");
        assertEquals(1, ((Number) first.get("arrayForm")).intValue(), "#377: raw array-form $cond without an inclusion flag");
        assertEquals(1, ((Number) first.get("objectForm")).intValue(), "#377: raw object-form $cond without an inclusion flag");
        assertEquals(1, ((Number) first.get("ref")).intValue(), "$field reference in a computed-only $project");
        assertEquals(0, ((Number) res.get(3).get("arrayForm")).intValue());
        assertEquals(0, ((Number) res.get(3).get("objectForm")).intValue());
    }

    /** {_id: 0} inside a computed-only $project drops _id like it does in strict inclusion mode. */
    @Test
    public void computedOnlyProjectHonoursIdExclusion() throws Exception {
        insertFourDocs();
        Map<String, Object> isOk = Doc.of("$eq", List.of("$result", "OK"));

        List<Map<String, Object>> res = new AggregateMongoCommand(drv).setDb(DB).setColl(COLL)
                .setPipeline(List.of(
                        Doc.of("$sort", Doc.of("counter", 1)),
                        Doc.of("$project", Doc.of("_id", 0, "flag", Doc.of("$cond", List.of(isOk, 1, 0))))))
                .execute();

        assertEquals(4, res.size());
        assertEquals(Set.of("flag"), res.get(0).keySet(), "#378: {_id: 0} must be honoured in a computed-only $project");
        assertEquals(1, ((Number) res.get(0).get("flag")).intValue());
        assertEquals(0, ((Number) res.get(2).get("flag")).intValue());
    }

    /** A pure exclusion spec ({field: 0}) is still exclusion mode: everything else stays. */
    @Test
    public void exclusionOnlyProjectKeepsTheRemainingFields() throws Exception {
        insertFourDocs();

        List<Map<String, Object>> res = new AggregateMongoCommand(drv).setDb(DB).setColl(COLL)
                .setPipeline(List.of(
                        Doc.of("$sort", Doc.of("counter", 1)),
                        Doc.of("$project", Doc.of("result", 0))))
                .execute();

        assertEquals(4, res.size());
        assertEquals(Set.of("_id", "counter"), res.get(0).keySet(), "exclusion mode must keep the non-excluded fields");
        assertEquals(1, ((Number) res.get(0).get("counter")).intValue());
    }
}
