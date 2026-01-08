package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("core")
public class CollationTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void queryTest(Morphium morphium) throws Exception {
        try (morphium) {
            if (morphium.getDriver().isInMemoryBackend()) {
                log.info("Collation does not work properly with InMemory backend - skipping");
                return;
            }

            log.info("==========================> Running with: " + morphium.getDriver().getName());
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForConditionToBecomeTrue(1000, "collection not deleted", ()->!morphium.exists(UncachedObject.class));
            morphium.store(new UncachedObject("A", 1));
            morphium.store(new UncachedObject("B", 1));
            morphium.store(new UncachedObject("C", 1));
            morphium.store(new UncachedObject("a", 1));
            morphium.store(new UncachedObject("b", 1));
            morphium.store(new UncachedObject("c", 1));
            TestUtils.waitForConditionToBecomeTrue(2500, "store failed", ()->TestUtils.countUC(morphium) == 6);
            Collation col = new Collation("de", false, Collation.CaseFirst.LOWER, Collation.Strength.TERTIARY, false, Collation.Alternate.SHIFTED, Collation.MaxVariable.SPACE, false, false);
            assert(col.getLocale().equals("de"));
            assert(!col.getCaseLevel());
            assert(col.getCaseFirst().equals(Collation.CaseFirst.LOWER));
            assert(!col.getNumericOrdering());
            assert(!col.getBackwards());
            assert(!col.getNormalization());
            assert(col.getStrength().equals(Collation.Strength.TERTIARY));
            assert(col.getAlternate().equals(Collation.Alternate.SHIFTED));
            assert(col.getMaxVariable().equals(Collation.MaxVariable.SPACE));
            List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).setCollation(col).sort("strValue").asList();
            String result = "";

            for (UncachedObject u : lst) {
                log.info("value: " + u.getStrValue());
                result += u.getStrValue();
            }

            assert(result.equals("aAbBcC")) : "Wrong ordering: " + result;
            col.normalization(true)
               .numericOrdering(true)
               .backwards(true)
               .alternate(Collation.Alternate.NON_IGNORABLE)
               .strength(Collation.Strength.SECONDARY)
               .maxVariable(Collation.MaxVariable.PUNCT)
               .caseLevel(true)
               .caseFirst(Collation.CaseFirst.UPPER);
            assert(col.getLocale().equals("de"));
            assert(col.getCaseLevel());
            assert(col.getCaseFirst().equals(Collation.CaseFirst.UPPER));
            assert(col.getNumericOrdering());
            assert(col.getBackwards());
            assert(col.getNormalization());
            assert(col.getStrength().equals(Collation.Strength.SECONDARY));
            assert(col.getAlternate().equals(Collation.Alternate.NON_IGNORABLE));
            assert(col.getMaxVariable().equals(Collation.MaxVariable.PUNCT));
            assertNotNull(col.getMaxVariable().getMongoText());
            ;
            assertNotNull(col.getAlternate().getMongoText());
            ;
            assert(col.getStrength().getMongoValue() != 0);
            assertNotNull(col.getCaseFirst().getMongoText());
            ;
            log.info("Query: " + Utils.toJsonString(col.toQueryObject()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void countTest(Morphium morphium) throws Exception {
        try (morphium) {
            if (morphium.getDriver().isInMemoryBackend()) {
                log.info("Collation does not work properly with InMemory backend - skipping");
                return;
            }

            log.info("==========================> Running with: " + morphium.getDriver().getName());
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForWrites(morphium, log);
            morphium.store(new UncachedObject("A", 1));
            morphium.store(new UncachedObject("B", 1));
            morphium.store(new UncachedObject("C", 1));
            morphium.store(new UncachedObject("a", 1));
            morphium.store(new UncachedObject("b", 1));
            morphium.store(new UncachedObject("c", 1));
            TestUtils.waitForConditionToBecomeTrue(3000, "Objects not persisted",
                () -> morphium.createQueryFor(UncachedObject.class).countAll() == 6);
            long count = morphium.createQueryFor(UncachedObject.class).setCollation(new Collation().locale("de").strength(Collation.Strength.PRIMARY)).f("str_value").eq("a").countAll();
            assertEquals(2, count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateTest(Morphium morphium) throws Exception {
        try (morphium) {
            if (morphium.getDriver().isInMemoryBackend()) {
                log.info("Collation does not work properly with InMemory backend - skipping");
                return;
            }

            log.info("==========================> Running with: " + morphium.getDriver().getName());
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForWrites(morphium, log);
            morphium.store(new UncachedObject("A", 1));
            morphium.store(new UncachedObject("B", 1));
            morphium.store(new UncachedObject("C", 1));
            morphium.store(new UncachedObject("a", 1));
            morphium.store(new UncachedObject("b", 1));
            morphium.store(new UncachedObject("c", 1));
            TestUtils.waitForConditionToBecomeTrue(3000, "Objects not persisted",
                () -> morphium.createQueryFor(UncachedObject.class).countAll() == 6);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).setCollation(new Collation().locale("de").strength(Collation.Strength.PRIMARY)).f("str_value").eq("a");
            morphium.inc(q, UncachedObject.Fields.counter, 1, false, true);
            TestUtils.waitForConditionToBecomeTrue(3000, "Inc operation not persisted",
                () -> {
                    var obj = morphium.createQueryFor(UncachedObject.class).f("str_value").eq("a").f("counter").eq(2).get();
                    return obj != null;
                });

            for (UncachedObject u : q.asIterable()) {
                assert(u.getCounter() == 2);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void delTest(Morphium morphium) throws Exception {
        try (morphium) {
            if (morphium.getDriver().isInMemoryBackend()) {
                log.info("Collation does not work properly with InMemory backend - skipping");
                return;
            }

            log.info("==========================> Running with: " + morphium.getDriver().getName());
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForWrites(morphium, log);
            morphium.store(new UncachedObject("A", 1));
            morphium.store(new UncachedObject("B", 1));
            morphium.store(new UncachedObject("C", 1));
            morphium.store(new UncachedObject("a", 1));
            morphium.store(new UncachedObject("b", 1));
            morphium.store(new UncachedObject("c", 1));
            TestUtils.waitForConditionToBecomeTrue(5000, "Did not write all properly", ()->TestUtils.countUC(morphium) == 6);

            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).setCollation(new Collation().locale("de").strength(Collation.Strength.PRIMARY)).f("strValue").eq("a");
            morphium.delete(q);
            TestUtils.waitForConditionToBecomeTrue(5000, "Did not delete properly", ()->TestUtils.countUC(morphium) == 4);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aggregateTest(Morphium morphium) throws Exception {
        try (morphium) {
            if (morphium.getDriver().isInMemoryBackend()) {
                log.info("Collation does not work properly with InMemory backend - skipping");
                return;
            }

            log.info("==========================> Running with: " + morphium.getDriver().getName());
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForWrites(morphium, log);
            morphium.store(new UncachedObject("A", 1));
            morphium.store(new UncachedObject("B", 1));
            morphium.store(new UncachedObject("C", 1));
            morphium.store(new UncachedObject("a", 1));
            morphium.store(new UncachedObject("b", 1));
            morphium.store(new UncachedObject("c", 1));
            TestUtils.waitForConditionToBecomeTrue(3000, "Objects not persisted",
                () -> morphium.createQueryFor(UncachedObject.class).countAll() == 6);
            Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
            agg.collation(new Collation().locale("de").strength(Collation.Strength.PRIMARY));
            agg.match(Expr.eq(Expr.field("str_value"), Expr.string("a")));
            List<Map> lst = agg.aggregate();
            assert(lst.size() == 2) : "Count wrong " + lst.size();
        }
    }

}
