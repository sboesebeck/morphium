package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class ExprFindServerTest extends MultiDriverTestBase {

    public static Stream<org.junit.jupiter.params.provider.Arguments> pooledInstancesOrPlaceholder() {
        List<org.junit.jupiter.params.provider.Arguments> pooled =
            MultiDriverTestBase.getMorphiumInstancesPooledOnly().collect(Collectors.toList());

        if (pooled.isEmpty()) {
            return Stream.of(org.junit.jupiter.params.provider.Arguments.of(new Object[] { null }));
        }

        return pooled.stream();
    }

    @ParameterizedTest
    @MethodSource("pooledInstancesOrPlaceholder")
    public void exprWorksOnServer(Morphium morphium) throws Exception {
        Assumptions.assumeTrue(morphium != null, "pooled driver unavailable");

        try (morphium) {
            createUncachedObjects(morphium, 100);
            TestUtils.waitForWrites(morphium, log);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            // With 0-based counters (0-99), counter >= 50 matches: 50, 51, ..., 99 = 50 values
            q.expr(Expr.gte(Expr.field(UncachedObject.Fields.counter), Expr.intExpr(50)));
            log.debug(Utils.toJsonString(q.toQueryObject()));

            List<UncachedObject> lst = q.asList();
            assertTrue(lst.size() == 50, "server returned wrong count for expr: " + lst.size());

            for (UncachedObject u : lst) {
                u.setDval(Math.random() * 100);
                morphium.store(u);
            }

            TestUtils.waitForWrites(morphium, log);
            q = q.q().expr(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.field(UncachedObject.Fields.dval)));
            lst = q.asList();
            assertTrue(!lst.isEmpty(), "expect at least one match");
            assertTrue(lst.size() < 100, "should not return all entries");

            for (UncachedObject u : lst) {
                assertTrue(u.getCounter() > u.getDval(), "expr filter failed");
            }
        }
    }
}
