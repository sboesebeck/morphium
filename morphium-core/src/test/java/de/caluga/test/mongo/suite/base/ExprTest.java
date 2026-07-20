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
}
