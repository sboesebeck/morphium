package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class AggregationExpressionTests {
    private Logger log = LoggerFactory.getLogger(AggregationExpressionTests.class);

    @Test
    public void test() {
        Expr e = Expr.add(Expr.field("the_field"), Expr.abs(Expr.field("test")), Expr.doubleExpr(128.0));
        Object o = e.toQueryObject();
        log.info(Utils.toJsonString(o));

        e = Expr.in(Expr.doubleExpr(1.2), Expr.arrayExpr(Expr.intExpr(12), Expr.doubleExpr(1.2), Expr.field("testfield")));
        log.info(Utils.toJsonString(e.toQueryObject()));

        e = Expr.zip(Arrays.asList(Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14)), Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14))), Expr.bool(true), Expr.field("test"));
        log.info(Utils.toJsonString(e.toQueryObject()));

        e = Expr.filter(Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14), Expr.string("asV")), "str", Expr.string("NEN"));
        log.info(Utils.toJsonString(e.toQueryObject()));
    }
}
