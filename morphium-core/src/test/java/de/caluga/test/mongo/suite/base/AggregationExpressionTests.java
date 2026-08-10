package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class AggregationExpressionTests {
    private final Logger log = LoggerFactory.getLogger(AggregationExpressionTests.class);

    @Test
    public void test() {
        Expr e = Expr.add(Expr.field("the_field"), Expr.abs(Expr.field("test")), Expr.doubleExpr(128.0));
        Object o = e.toQueryObject();
        String val = Utils.toJsonString(o);
        log.info(val);
        assert (val.equals("{ \"$add\" :  [ \"$the_field\", { \"$abs\" : \"$test\" } , 128.0] } "));

        e = Expr.in(Expr.doubleExpr(1.2), Expr.arrayExpr(Expr.intExpr(12), Expr.doubleExpr(1.2), Expr.field("testfield")));
        val = Utils.toJsonString(e.toQueryObject());
        log.info(val);
        assert (val.equals("{ \"$in\" :  [ 1.2,  [ 12, 1.2, \"$testfield\"]] } "));

        e = Expr.zip(Arrays.asList(Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14)), Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14))), Expr.bool(true), Expr.field("test"));
        val = Utils.toJsonString(e.toQueryObject());
        log.info(val);
        assert (val.equals("{ \"$zip\" : { \"inputs\" :  [  [ 1, 14],  [ 1, 14]], \"useLongestLength\" : true, \"defaults\" : \"$test\" }  } "));


        e = Expr.filter(Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14), Expr.string("asV")), "str", Expr.string("NEN"));
        val = Utils.toJsonString(e.toQueryObject());
        log.info(val);
        assert (val.equals("{ \"$filter\" : { \"input\" :  [ 1, 14, \"asV\"], \"as\" : \"str\", \"cond\" : \"NEN\" }  } "));
    }

    @Test
    public void inRejectsNonArrayOperand() {
        Expr missingArray = Expr.in(Expr.string("value"), Expr.field("missing"));
        Expr scalarArray = Expr.in(Expr.string("value"), Expr.string("value"));

        assertThrows(IllegalArgumentException.class, () -> missingArray.evaluate(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> scalarArray.evaluate(Map.of()));
    }

    @Test
    public void inSupportsListsJavaArraysAndNullElements() {
        Expr expression = Expr.in(Expr.nullExpr(), Expr.field("values"));

        assertTrue((Boolean) expression.evaluate(Map.of("values", Arrays.asList("value", null))));
        assertTrue((Boolean) expression.evaluate(Map.of("values", new String[]{"value", null})));
        assertFalse((Boolean) expression.evaluate(Map.of("values", List.of("value"))));
    }
}
