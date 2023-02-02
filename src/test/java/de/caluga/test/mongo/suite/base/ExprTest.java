package de.caluga.test.mongo.suite.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.Doc;

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
    public void eqNPETest() {
        Expr eq = Expr.eq(Expr.field("test"), Expr.string("tst"));
        Doc context = Doc.of("test", "tst");
        assertTrue((boolean) eq.evaluate(context));
        context.put("test", null);
        assertFalse((boolean) eq.evaluate(context));
    }
}
