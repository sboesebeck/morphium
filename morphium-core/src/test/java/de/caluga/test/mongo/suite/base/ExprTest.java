package de.caluga.test.mongo.suite.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    @Test
    public void reverseArray_doesNotMutateSource() {
        java.util.List<Object> source = new java.util.ArrayList<>(java.util.List.of(1, 2, 3));
        Doc ctx = Doc.of("arr", source);
        Object reversed = Expr.reverseArray(Expr.field("arr")).evaluate(ctx);
        assertEquals(java.util.List.of(3, 2, 1), reversed);
        assertEquals(java.util.List.of(1, 2, 3), source, "$reverseArray must not mutate its source list");
    }
}
