package de.caluga.test.mongo.suite.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.Doc;

@Tag("core")
public class ExprTest {
    @Test
    public void fieldPathExprTest() throws Exception {
        Expr fieldExpr = Expr.field("test.t2");
        Doc context = Doc.of("test", Doc.of("t2", "value"));
        var res = fieldExpr.evaluate(context);
        assertEquals("value", res);
    }

    @Test
    public void fieldExprTest() throws Exception {
        Expr fieldExpr = Expr.field("test");
        Doc context = Doc.of("test", "value");
        var res = fieldExpr.evaluate(context);
        assertEquals("value", res);
    }

    @Test
    public void neNPETest() {
        Expr ne = Expr.ne(Expr.field("test"), Expr.string("tst"));
        Doc context = Doc.of("test", "tst");
        assertFalse((boolean) ne.evaluate(context));
        context.put("test", null);
        assertTrue((boolean) ne.evaluate(context));
    }

    @Test
    public void neBooleanTest() {
        Expr ne = Expr.ne(Expr.field("test"), Expr.bool(false));
        Doc context = Doc.of("test", Boolean.FALSE);
        assertFalse((boolean) ne.evaluate(context));
        context.put("test", Expr.bool(true));
        assertTrue((boolean) ne.evaluate(context));
    }

    @Test
    public void eqNPETest() {
        Expr eq = Expr.eq(Expr.field("test"), Expr.string("tst"));
        Doc context = Doc.of("test", "tst");
        assertTrue((boolean) eq.evaluate(context));
        context.put("test", null);
        assertFalse((boolean) eq.evaluate(context));
    }

    // #246: $avg/$max/$min as single-argument expressions must reduce an array argument,
    // not return it unchanged (as $sum's single-arg form already does).

    @Test
    public void avgSingleArg_overArray_reducesToMean() {
        Doc ctx = Doc.of("scores", java.util.List.of(2, 4, 6));
        assertEquals(4.0, ((Number) Expr.avg(Expr.field("scores")).evaluate(ctx)).doubleValue(), 1e-9);
    }

    @Test
    public void maxSingleArg_overArray_reducesToMax() {
        Doc ctx = Doc.of("scores", java.util.List.of(2, 9, 6));
        assertEquals(9, Expr.max(Expr.field("scores")).evaluate(ctx));
    }

    @Test
    public void minSingleArg_overArray_reducesToMin() {
        Doc ctx = Doc.of("scores", java.util.List.of(5, 2, 6));
        assertEquals(2, Expr.min(Expr.field("scores")).evaluate(ctx));
    }

    // #253: Expr misc correctness cluster ($ln, $range descending, $reverseArray mutation).

    @Test
    public void ln_isNaturalLog_notLog1p() {
        // ln(e) == 1; log1p(e) would compute ln(1+e) != 1
        assertEquals(1.0, ((Number) Expr.ln(Expr.doubleExpr(Math.E)).evaluate(Doc.of())).doubleValue(), 1e-9);
    }

    @Test
    public void range_descending_producesCountdown() {
        Object res = Expr.range(Expr.intExpr(5), Expr.intExpr(1), Expr.intExpr(-1)).evaluate(Doc.of());
        assertEquals(java.util.List.of(5, 4, 3, 2), res);
    }

    // #250: date accessors must evaluate in UTC (MongoDB's default), $month is 1-12, and the ISO
    // operators must use real ISO-8601 week-date fields.
    //
    // Reference instant: 2026-03-15T13:45:30.500Z — a Sunday, day-of-year 74.
    // 2026-01-01 is a Thursday, so ISO week 1 is Mon 2025-12-29..Sun 2026-01-04 and 2026-03-15 is
    // the last day of ISO week 11. MongoDB's Sunday-based $week: first Sunday is Jan 4, so
    // ((74-4)/7)+1 = 11.

    private java.util.Date refInstant() {
        return java.util.Date.from(java.time.Instant.parse("2026-03-15T13:45:30.500Z"));
    }

    private int evalDate(Expr e) {
        return ((Number) e.evaluate(Doc.of("d", refInstant()))).intValue();
    }

    @Test
    public void dateAccessors_useUtcNotJvmDefaultTimezone() {
        assertEquals(13, evalDate(Expr.hour(Expr.field("d"))), "$hour must be evaluated in UTC");
        assertEquals(45, evalDate(Expr.minute(Expr.field("d"))));
        assertEquals(30, evalDate(Expr.second(Expr.field("d"))));
        assertEquals(500, evalDate(Expr.millisecond(Expr.field("d"))));
    }

    @Test
    public void month_isOneBased() {
        assertEquals(3, evalDate(Expr.month(Expr.field("d"))), "$month is 1-12 in MongoDB, not 0-based");
    }

    @Test
    public void isoDayOfWeek_isMondayOneToSundaySeven() {
        assertEquals(7, evalDate(Expr.isoDayOfWeek(Expr.field("d"))), "Sunday is 7 in ISO numbering");
        assertEquals(1, evalDate(Expr.dayOfWeek(Expr.field("d"))), "$dayOfWeek keeps Sunday=1");
    }

    @Test
    public void isoWeek_and_isoWeekYear_useIsoWeekDateFields() {
        assertEquals(11, evalDate(Expr.isoWeek(Expr.field("d"))), "$isoWeek is the ISO week-of-year");
        assertEquals(2026, evalDate(Expr.isoWeekYear(Expr.field("d"))), "$isoWeekYear is a 4-digit year");
    }

    @Test
    public void remainingDateAccessors_matchReferenceInstant() {
        assertEquals(2026, evalDate(Expr.year(Expr.field("d"))));
        assertEquals(15, evalDate(Expr.dayOfMonth(Expr.field("d"))));
        assertEquals(74, evalDate(Expr.dayOfYear(Expr.field("d"))));
        assertEquals(11, evalDate(Expr.week(Expr.field("d"))), "$week is Sunday-based, 0-53");
    }

    // #260: $dateFromParts/$isoDateFromParts hit the MapOpExpr-without-evaluate() path and returned
    // their own JSON shape instead of a computed Date.

    @Test
    public void dateFromParts_computesDate_viaFluentBuilder() {
        Expr e = Expr.dateFromParts(Expr.intExpr(2026), Expr.intExpr(3), Expr.intExpr(15),
                Expr.intExpr(13), Expr.intExpr(45), Expr.intExpr(30));
        Object result = e.evaluate(Doc.of());
        assertInstanceOf(java.util.Date.class, result,
            "$dateFromParts must return a Date, not its own JSON shape");
        assertEquals(java.time.Instant.parse("2026-03-15T13:45:30Z"), ((java.util.Date) result).toInstant());
    }

    @Test
    public void dateFromParts_computesDate_viaJsonPipeline() {
        Expr e = Expr.parse(Doc.of("$dateFromParts", Doc.of("year", 2026, "month", 3, "day", 15)));
        Object result = e.evaluate(Doc.of());
        assertInstanceOf(java.util.Date.class, result,
            "$dateFromParts parsed from JSON must return a Date");
        assertEquals(java.time.Instant.parse("2026-03-15T00:00:00Z"), ((java.util.Date) result).toInstant());
    }

    @Test
    public void isoDateFromParts_computesDate() {
        // ISO week 11 of 2026, ISO day 7 (Sunday) is 2026-03-15
        Expr e = Expr.isoDateFromParts(Expr.intExpr(2026), Expr.intExpr(11), Expr.intExpr(7));
        Object result = e.evaluate(Doc.of());
        assertInstanceOf(java.util.Date.class, result,
            "$isoDateFromParts must return a Date, not its own JSON shape");
        assertEquals(java.time.Instant.parse("2026-03-15T00:00:00Z"), ((java.util.Date) result).toInstant());
    }

    @Test
    public void reverseArray_doesNotMutateSource() {
        java.util.List<Object> source = new java.util.ArrayList<>(java.util.List.of(1, 2, 3));
        Doc ctx = Doc.of("arr", source);
        Object reversed = Expr.reverseArray(Expr.field("arr")).evaluate(ctx);
        assertEquals(java.util.List.of(3, 2, 1), reversed);
        assertEquals(java.util.List.of(1, 2, 3), source, "$reverseArray must not mutate its source list");
    }

    // ------------------------------------------------------------------
    // #255: missing and stubbed aggregation expression operators
    // ------------------------------------------------------------------

    private double evalNum(Expr e) {
        return ((Number) e.evaluate(Doc.of())).doubleValue();
    }

    // --- hyperbolic trig ---

    @Test
    public void hyperbolicTrig_plainForms() {
        assertEquals(Math.sinh(1.0), evalNum(Expr.sinh(Expr.doubleExpr(1.0))), 1e-12);
        assertEquals(Math.cosh(1.0), evalNum(Expr.cosh(Expr.doubleExpr(1.0))), 1e-12);
        assertEquals(Math.tanh(1.0), evalNum(Expr.tanh(Expr.doubleExpr(1.0))), 1e-12);
    }

    @Test
    public void inverseHyperbolicTrig_realImplementations() {
        // asinh(1) = ln(1+sqrt(2)); the old implementation computed sinh instead
        assertEquals(Math.log(1 + Math.sqrt(2)), evalNum(Expr.asinh(Expr.doubleExpr(1.0))), 1e-12);
        // acosh(2) = ln(2+sqrt(3)); was a stub returning null
        assertEquals(Math.log(2 + Math.sqrt(3)), evalNum(Expr.acosh(Expr.doubleExpr(2.0))), 1e-12);
        // atanh(0.5) = 0.5*ln(3); was a stub logging + returning 0
        assertEquals(0.5 * Math.log(3), evalNum(Expr.atanh(Expr.doubleExpr(0.5))), 1e-12);
    }

    @Test
    public void atanh_parsesAsOneArgOperator() {
        Expr e = Expr.parse(Doc.of("$atanh", 0.5));
        assertEquals(0.5 * Math.log(3), ((Number) e.evaluate(Doc.of())).doubleValue(), 1e-12);
    }

    @Test
    public void degreesToRadians_correctlySpelled_andParseable() {
        assertEquals(Math.PI, evalNum(Expr.degreesToRadians(Expr.doubleExpr(180.0))), 1e-12);
        // the misspelled internal name "degreesToRadian" made JSON dispatch fail
        Expr parsed = Expr.parse(Doc.of("$degreesToRadians", 180.0));
        assertEquals(Math.PI, ((Number) parsed.evaluate(Doc.of())).doubleValue(), 1e-12);
    }

    // --- $round (2-arg form, half-to-even like MongoDB) ---

    @Test
    public void round_isHalfToEven() {
        assertEquals(2.0, evalNum(Expr.round(Expr.doubleExpr(2.5))), 0.0, "$round rounds half to even");
        assertEquals(4.0, evalNum(Expr.round(Expr.doubleExpr(3.5))), 0.0);
    }

    @Test
    public void round_withPlace() {
        assertEquals(1.23, evalNum(Expr.round(Expr.doubleExpr(1.234), Expr.intExpr(2))), 1e-12);
        assertEquals(1.2, evalNum(Expr.round(Expr.doubleExpr(1.25), Expr.intExpr(1))), 1e-12);
        // negative place rounds to tens/hundreds; integer input stays integral
        assertEquals(1200, Expr.round(Expr.intExpr(1234), Expr.intExpr(-2)).evaluate(Doc.of()));
    }

    @Test
    public void round_twoArg_parsesFromJson() {
        Expr e = Expr.parse(Doc.of("$round", java.util.List.of("$x", 1)));
        assertEquals(2.4, ((Number) e.evaluate(Doc.of("x", 2.35))).doubleValue(), 1e-12);
    }

    // --- string byte/codepoint operators ---
    // "a😀b" is a-EMOJI-b: 3 code points, 4 UTF-16 chars, 6 UTF-8 bytes

    private static final String EMO = "a😀b";

    @Test
    public void strLenBytes_and_strLenCP() {
        assertEquals(6, Expr.strLenBytes(Expr.string(EMO)).evaluate(Doc.of()));
        assertEquals(3, Expr.strLenCP(Expr.string(EMO)).evaluate(Doc.of()));
    }

    @Test
    public void strLenBytes_throwsOnNonString() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> Expr.strLenBytes(Expr.field("missing")).evaluate(Doc.of()));
    }

    @Test
    public void substrCP_and_substrBytes() {
        assertEquals("😀", Expr.substrCP(Expr.string(EMO), Expr.intExpr(1), Expr.intExpr(1)).evaluate(Doc.of()));
        assertEquals("bc", Expr.substrCP(Expr.string("abc"), Expr.intExpr(1), Expr.intExpr(10)).evaluate(Doc.of()),
            "count past the end is clamped");
        assertEquals("😀", Expr.substrBytes(Expr.string(EMO), Expr.intExpr(1), Expr.intExpr(4)).evaluate(Doc.of()));
    }

    @Test
    public void substrBytes_throwsWhenSplittingUtf8Char() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> Expr.substrBytes(Expr.string(EMO), Expr.intExpr(0), Expr.intExpr(2)).evaluate(Doc.of()));
    }

    @Test
    public void indexOfBytes_and_indexOfCP() {
        assertEquals(5, Expr.indexOfBytes(Expr.string(EMO), Expr.string("b"), null, null).evaluate(Doc.of()));
        assertEquals(2, Expr.indexOfCP(Expr.string(EMO), Expr.string("b"), null, null).evaluate(Doc.of()));
        assertEquals(-1, Expr.indexOfCP(Expr.string("abc"), Expr.string("x"), null, null).evaluate(Doc.of()));
        // null string input yields null (MongoDB semantics)
        assertEquals(null, Expr.indexOfCP(Expr.field("missing"), Expr.string("x"), null, null).evaluate(Doc.of()));
        // start/end restrict the search range
        assertEquals(2, Expr.indexOfCP(Expr.string("aaa"), Expr.string("a"), Expr.intExpr(2), null).evaluate(Doc.of()));
        assertEquals(-1, Expr.indexOfCP(Expr.string("abca"), Expr.string("a"), Expr.intExpr(1), Expr.intExpr(3)).evaluate(Doc.of()));
    }

    @Test
    public void indexOfCP_parsesTwoArgForm() {
        Expr e = Expr.parse(Doc.of("$indexOfCP", java.util.List.of("$s", "b")));
        assertEquals(2, e.evaluate(Doc.of("s", EMO)));
    }

    @Test
    public void strcasecmp_comparesCaseInsensitive() {
        assertEquals(0, Expr.strcasecmp(Expr.string("Hello"), Expr.string("hello")).evaluate(Doc.of()));
        assertEquals(-1, Expr.strcasecmp(Expr.string("abc"), Expr.string("ABD")).evaluate(Doc.of()));
        assertEquals(1, Expr.strcasecmp(Expr.string("b"), Expr.string("A")).evaluate(Doc.of()));
    }

    // --- $toDate / $type ---

    @Test
    public void toDate_convertsCommonTypes() {
        java.util.Date d = refInstant();
        assertEquals(d, Expr.toDate(Expr.field("d")).evaluate(Doc.of("d", d)));
        assertEquals(d, Expr.toDate(Expr.field("l")).evaluate(Doc.of("l", d.getTime())));
        assertEquals(d, Expr.toDate(Expr.string("2026-03-15T13:45:30.500Z")).evaluate(Doc.of()));
        assertEquals(java.util.Date.from(java.time.Instant.parse("2026-03-15T00:00:00Z")),
            Expr.toDate(Expr.string("2026-03-15")).evaluate(Doc.of()));
        assertEquals(null, Expr.toDate(Expr.field("missing")).evaluate(Doc.of()));
    }

    @Test
    public void type_returnsBsonTypeNames() {
        assertEquals("string", Expr.type(Expr.string("x")).evaluate(Doc.of()));
        assertEquals("int", Expr.type(Expr.intExpr(1)).evaluate(Doc.of()));
        assertEquals("long", Expr.type(Expr.field("l")).evaluate(Doc.of("l", 5L)));
        assertEquals("double", Expr.type(Expr.doubleExpr(1.5)).evaluate(Doc.of()));
        assertEquals("bool", Expr.type(Expr.bool(true)).evaluate(Doc.of()));
        assertEquals("date", Expr.type(Expr.field("d")).evaluate(Doc.of("d", refInstant())));
        assertEquals("array", Expr.type(Expr.field("a")).evaluate(Doc.of("a", java.util.List.of(1))));
        assertEquals("object", Expr.type(Expr.field("o")).evaluate(Doc.of("o", Doc.of("x", 1))));
        assertEquals("null", Expr.type(Expr.field("missing")).evaluate(Doc.of()));
        assertEquals("objectId", Expr.type(Expr.field("id")).evaluate(Doc.of("id", new de.caluga.morphium.driver.MorphiumId())));
    }

    // --- array operators ---

    @Test
    public void map_appliesExpressionToEachElement() {
        Doc ctx = Doc.of("arr", java.util.List.of(1, 2, 3));
        Object res = Expr.map(Expr.field("arr"), Expr.string("x"), Expr.multiply(Expr.field("x"), Expr.intExpr(2))).evaluate(ctx);
        assertEquals(java.util.List.of(2.0, 4.0, 6.0), res);
    }

    @Test
    public void map_parsesFromJson_withDollarDollarVar() {
        Expr e = Expr.parse(Doc.of("$map", Doc.of("input", "$arr", "as", "x", "in", Doc.of("$multiply", java.util.List.of("$$x", 2)))));
        assertEquals(java.util.List.of(2.0, 4.0, 6.0), e.evaluate(Doc.of("arr", java.util.List.of(1, 2, 3))));
    }

    @Test
    public void map_nullInput_returnsNull() {
        assertEquals(null, Expr.map(Expr.field("missing"), Expr.string("x"), Expr.field("x")).evaluate(Doc.of()));
    }

    @Test
    public void arrayToObject_supportsBothInputShapes() {
        Doc pairCtx = Doc.of("p", java.util.List.of(java.util.List.of("a", 1), java.util.List.of("b", 2)));
        assertEquals(Doc.of("a", 1, "b", 2), Expr.arrayToObject(Expr.field("p")).evaluate(pairCtx));
        Doc kvCtx = Doc.of("p", java.util.List.of(Doc.of("k", "a", "v", 1)));
        assertEquals(Doc.of("a", 1), Expr.arrayToObject(Expr.field("p")).evaluate(kvCtx));
        assertEquals(null, Expr.arrayToObject(Expr.field("missing")).evaluate(Doc.of()));
    }

    @Test
    public void firstAndLast_actAsArrayOperators() {
        Doc ctx = Doc.of("arr", java.util.List.of(1, 2, 3));
        assertEquals(1, Expr.first(Expr.field("arr")).evaluate(ctx));
        assertEquals(3, Expr.last(Expr.field("arr")).evaluate(ctx));
        assertEquals(null, Expr.first(Expr.field("missing")).evaluate(Doc.of()));
        assertEquals(null, Expr.first(Expr.field("e")).evaluate(Doc.of("e", java.util.List.of())));
    }

    @Test
    public void pushAndAddToSet_throwOutsideGroup() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> Expr.push(Expr.field("x")).evaluate(Doc.of("x", 1)));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> Expr.addToSet(Expr.field("x")).evaluate(Doc.of("x", 1)));
    }

    @Test
    public void sortArray_scalarSortBy() {
        Doc ctx = Doc.of("arr", java.util.List.of(3, 1, 2));
        assertEquals(java.util.List.of(1, 2, 3), Expr.sortArray(Expr.field("arr"), Expr.intExpr(1)).evaluate(ctx));
        assertEquals(java.util.List.of(3, 2, 1), Expr.sortArray(Expr.field("arr"), Expr.intExpr(-1)).evaluate(ctx));
    }

    @Test
    public void sortArray_docSortBy_viaJson() {
        Doc ctx = Doc.of("arr", java.util.List.of(Doc.of("n", 2), Doc.of("n", 1), Doc.of("n", 3)));
        Expr e = Expr.parse(Doc.of("$sortArray", Doc.of("input", "$arr", "sortBy", Doc.of("n", -1))));
        assertEquals(java.util.List.of(Doc.of("n", 3), Doc.of("n", 2), Doc.of("n", 1)), e.evaluate(ctx));
    }

    @Test
    public void firstN_lastN_maxN_minN() {
        Doc ctx = Doc.of("arr", java.util.List.of(3, 1, 7));
        assertEquals(java.util.List.of(3, 1), Expr.firstN(Expr.field("arr"), Expr.intExpr(2)).evaluate(ctx));
        assertEquals(java.util.List.of(1, 7), Expr.lastN(Expr.field("arr"), Expr.intExpr(2)).evaluate(ctx));
        assertEquals(java.util.List.of(7, 3), Expr.maxN(Expr.field("arr"), Expr.intExpr(2)).evaluate(ctx));
        assertEquals(java.util.List.of(1, 3), Expr.minN(Expr.field("arr"), Expr.intExpr(2)).evaluate(ctx));
        // n larger than the array returns the whole array
        assertEquals(java.util.List.of(3, 1, 7), Expr.firstN(Expr.field("arr"), Expr.intExpr(9)).evaluate(ctx));
    }

    @Test
    public void firstN_parsesFromJson() {
        Expr e = Expr.parse(Doc.of("$firstN", Doc.of("n", 2, "input", "$arr")));
        assertEquals(java.util.List.of(3, 1), e.evaluate(Doc.of("arr", java.util.List.of(3, 1, 7))));
    }

    // --- set operators (were log+null stubs) ---

    @Test
    public void setOperators_computeRealResults() {
        Doc ctx = Doc.of("a", java.util.List.of(1, 2, 3), "b", java.util.List.of(2, 4));
        assertEquals(java.util.List.of(1, 3), Expr.setDifference(Expr.field("a"), Expr.field("b")).evaluate(ctx));
        assertEquals(java.util.List.of(2), Expr.setIntersection(Expr.field("a"), Expr.field("b")).evaluate(ctx));
        assertTrue((boolean) Expr.setIsSubset(Expr.field("b2"), Expr.field("a"))
            .evaluate(Doc.of("a", java.util.List.of(1, 2, 3), "b2", java.util.List.of(1, 2))));
        assertFalse((boolean) Expr.setIsSubset(Expr.field("a"), Expr.field("b")).evaluate(ctx));
        assertTrue((boolean) Expr.setEquals(Expr.field("x"), Expr.field("y"))
            .evaluate(Doc.of("x", java.util.List.of(1, 2), "y", java.util.List.of(2, 1, 1))));
        Object union = Expr.setUnion(Expr.field("a"), Expr.field("b")).evaluate(ctx);
        assertEquals(new java.util.HashSet<>(java.util.List.of(1, 2, 3, 4)), new java.util.HashSet<>((java.util.List<?>) union),
            "$setUnion must union the elements, not collect the arrays themselves");
    }

    // --- data size operators ---

    @Test
    public void binarySize_and_bsonSize() {
        assertEquals(3, Expr.binarySize(Expr.string("abc")).evaluate(Doc.of()));
        assertEquals(null, Expr.binarySize(Expr.field("missing")).evaluate(Doc.of()));
        Doc doc = Doc.of("a", 1);
        Object size = Expr.bsonSize(Expr.field("o")).evaluate(Doc.of("o", doc));
        assertEquals(de.caluga.morphium.driver.bson.BsonEncoder.encodeDocument(doc).length, ((Number) size).intValue());
        assertEquals(null, Expr.bsonSize(Expr.field("missing")).evaluate(Doc.of()));
    }

    // --- date arithmetic ($dateAdd/$dateSubtract/$dateDiff/$dateTrunc), all UTC ---

    private java.util.Date d(String iso) {
        return java.util.Date.from(java.time.Instant.parse(iso));
    }

    @Test
    public void dateAdd_addsUnits_withMonthClamping() {
        Object res = Expr.dateAdd(Expr.field("d"), Expr.string("month"), Expr.intExpr(1)).evaluate(Doc.of("d", d("2026-01-31T00:00:00Z")));
        assertEquals(d("2026-02-28T00:00:00Z"), res, "$dateAdd clamps to the last day of the target month");
        assertEquals(d("2026-03-16T01:00:00Z"),
            Expr.dateAdd(Expr.field("d"), Expr.string("hour"), Expr.intExpr(2)).evaluate(Doc.of("d", d("2026-03-15T23:00:00Z"))));
        assertEquals(null, Expr.dateAdd(Expr.field("missing"), Expr.string("day"), Expr.intExpr(1)).evaluate(Doc.of()));
    }

    @Test
    public void dateSubtract_subtractsUnits() {
        assertEquals(d("2026-02-28T00:00:00Z"),
            Expr.dateSubtract(Expr.field("d"), Expr.string("month"), Expr.intExpr(1)).evaluate(Doc.of("d", d("2026-03-31T00:00:00Z"))));
    }

    @Test
    public void dateAdd_parsesFromJson() {
        Expr e = Expr.parse(Doc.of("$dateAdd", Doc.of("startDate", "$d", "unit", "hour", "amount", 2)));
        assertEquals(d("2026-03-15T15:45:30.500Z"), e.evaluate(Doc.of("d", refInstant())));
    }

    @Test
    public void dateDiff_countsUnitBoundariesCrossed() {
        // 23:59 -> 00:01 next day crosses one midnight: 1 day, although only 2 minutes elapsed
        assertEquals(1L, Expr.dateDiff(Expr.field("s"), Expr.field("e"), Expr.string("day"))
            .evaluate(Doc.of("s", d("2026-01-01T23:59:00Z"), "e", d("2026-01-02T00:01:00Z"))));
        assertEquals(2L, Expr.dateDiff(Expr.field("s"), Expr.field("e"), Expr.string("hour"))
            .evaluate(Doc.of("s", d("2026-01-01T10:59:00Z"), "e", d("2026-01-01T12:01:00Z"))));
        assertEquals(1L, Expr.dateDiff(Expr.field("s"), Expr.field("e"), Expr.string("month"))
            .evaluate(Doc.of("s", d("2026-01-31T00:00:00Z"), "e", d("2026-02-01T00:00:00Z"))));
        assertEquals(1L, Expr.dateDiff(Expr.field("s"), Expr.field("e"), Expr.string("year"))
            .evaluate(Doc.of("s", d("2025-12-31T00:00:00Z"), "e", d("2026-01-01T00:00:00Z"))));
        // default startOfWeek is Sunday: Sat 2026-03-14 -> Sun 2026-03-15 crosses a week boundary
        assertEquals(1L, Expr.dateDiff(Expr.field("s"), Expr.field("e"), Expr.string("week"))
            .evaluate(Doc.of("s", d("2026-03-14T12:00:00Z"), "e", d("2026-03-15T12:00:00Z"))));
        assertEquals(null, Expr.dateDiff(Expr.field("missing"), Expr.field("e"), Expr.string("day")).evaluate(Doc.of("e", refInstant())));
    }

    @Test
    public void dateTrunc_truncatesToUnit() {
        Doc ctx = Doc.of("d", refInstant()); // 2026-03-15T13:45:30.500Z, a Sunday
        assertEquals(d("2026-03-15T00:00:00Z"), Expr.dateTrunc(Expr.field("d"), Expr.string("day")).evaluate(ctx));
        assertEquals(d("2026-03-01T00:00:00Z"), Expr.dateTrunc(Expr.field("d"), Expr.string("month")).evaluate(ctx));
        assertEquals(d("2026-01-01T00:00:00Z"), Expr.dateTrunc(Expr.field("d"), Expr.string("year")).evaluate(ctx));
        // Wednesday 2026-03-18, default startOfWeek Sunday -> back to Sunday 2026-03-15
        assertEquals(d("2026-03-15T00:00:00Z"),
            Expr.dateTrunc(Expr.field("d"), Expr.string("week")).evaluate(Doc.of("d", d("2026-03-18T10:00:00Z"))));
        assertEquals(null, Expr.dateTrunc(Expr.field("missing"), Expr.string("day")).evaluate(Doc.of()));
    }

    @Test
    public void dateTrunc_withBinSize_viaJson() {
        // 6h bins anchored at midnight: 13:45 -> 12:00
        Expr e = Expr.parse(Doc.of("$dateTrunc", Doc.of("date", "$d", "unit", "hour", "binSize", 6)));
        assertEquals(d("2026-03-15T12:00:00Z"), e.evaluate(Doc.of("d", refInstant())));
    }

    // --- misc: $rand, $sampleRate ---

    @Test
    public void rand_returnsDoubleInUnitInterval() {
        for (int i = 0; i < 10; i++) {
            double v = ((Number) Expr.rand().evaluate(Doc.of())).doubleValue();
            assertTrue(v >= 0.0 && v < 1.0);
        }
        double parsed = ((Number) Expr.parse(Doc.of("$rand", Doc.of())).evaluate(Doc.of())).doubleValue();
        assertTrue(parsed >= 0.0 && parsed < 1.0);
    }

    @Test
    public void sampleRate_boundaryValues() {
        assertTrue((boolean) Expr.sampleRate(Expr.doubleExpr(1.0)).evaluate(Doc.of()));
        assertFalse((boolean) Expr.sampleRate(Expr.doubleExpr(0.0)).evaluate(Doc.of()));
    }

    // --- $median / $percentile as plain expressions ---

    @Test
    public void median_and_percentile() {
        Doc ctx = Doc.of("v", java.util.List.of(1, 5, 2, 3, 4));
        assertEquals(3.0, ((Number) Expr.median(Expr.field("v")).evaluate(ctx)).doubleValue(), 0.0);
        Doc ctx10 = Doc.of("v", java.util.List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Object res = Expr.percentile(Expr.field("v"), Expr.arrayExpr(Expr.doubleExpr(0.5), Expr.doubleExpr(0.9))).evaluate(ctx10);
        assertEquals(java.util.List.of(5.0, 9.0), res);
        assertEquals(null, Expr.median(Expr.field("missing")).evaluate(Doc.of()));
    }

    // --- $function / $accumulator: no JS engine -> loud failure, not silent JSON ---

    @Test
    public void functionMapForm_and_accumulator_throwLoudly() {
        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
            () -> Expr.function("function(x){return x;}", Expr.arrayExpr(Expr.intExpr(1))).evaluate(Doc.of()));
        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
            () -> Expr.accumulator("function(){}", "function(s,x){}", Expr.arrayExpr(Expr.intExpr(1)), "function(a,b){}").evaluate(Doc.of()));
    }

    // --- parse: a map whose first key is no $-operator is a document literal ---

    @Test
    public void parse_documentLiteral_evaluatesPerField() {
        Expr e = Expr.parse(Doc.of("a", 1, "b", "$x"));
        assertEquals(Doc.of("a", 1, "b", 42), e.evaluate(Doc.of("x", 42)));
    }
}
