package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Expr;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Tag("core")
public class ExpEvaluationTest {

    @Test
    public void fieldExprTest() {
        Map<String, Object> context = UtilsMap.of("fld1", "test");
        Expr f = Expr.field("fld1");

        Object v = f.evaluate(context);
        assert (v.equals(context.get("fld1")));

    }

    @Test
    public void divideTest() {
        Map<String, Object> context = UtilsMap.of("fld1", (Object) 42, "fld2", 2);
        Object r = Expr.divide(Expr.field("fld1"), Expr.intExpr(3)).evaluate(context);
        assert (r != null && r.equals(14.0));
    }
}
