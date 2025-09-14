package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

@Tag("core")
public class AggregationExpQuery extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testQuery(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Thread.sleep(1000);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.expr(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.intExpr(50)));
            log.debug(Utils.toJsonString(q.toQueryObject()));
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

}
