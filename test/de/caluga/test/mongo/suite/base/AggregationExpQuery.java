package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

public class AggregationExpQuery extends MorphiumTestBase {
    @Test
    public void testQuery() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(1000);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.expr(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.intExpr(50)));
        log.info(Utils.toJsonString(q.toQueryObject()));
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 50) : "Size wrong: " + lst.size();


        for (UncachedObject u : q.q().asList()) {
            u.setDval(Math.random() * 100);
            morphium.store(u);
        }

        q = q.q().expr(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.field(UncachedObject.Fields.dval)));
        lst = q.asList();
        assert (lst.size() > 0);
        assert (lst.size() < 100);
        for (UncachedObject u : lst) {
            assert (u.getCounter() > u.getDval());
        }
    }

}
