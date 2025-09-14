package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.regex.Pattern;

@Tag("core")
public class RegexTests extends MorphiumTestBase {

    @Test
    public void simpleRegexTests() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        long tm = TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->q.countAll() == 100);
        log.info("Written after " + tm + "ms");
        assertEquals (50L, q.f(UncachedObject.Fields.strValue).matches("VALUE.*").countAll());
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches("9$").asList();
        assert (lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue (o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternRegexTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        long tm = TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->q.countAll() == 100);
        log.info("Written after " + tm + "ms");
        assertTrue(q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("VALUE.*")).countAll() == 50);
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches(Pattern.compile("9$")).asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void regexOptionTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        long tm = TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->q.countAll() == 100);
        log.info("Written after " + tm + "ms");
        assertEquals (1, q.f(UncachedObject.Fields.strValue).matches("value 9$", "i").countAll());
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches("value .*9$", "i").asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternOptionTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        long tm = TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->q.countAll() == 100);
        log.info("Written after " + tm + "ms");
        assertEquals (1, q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("value 9$", Pattern.CASE_INSENSITIVE)).countAll());
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches(Pattern.compile("value .*9$", Pattern.CASE_INSENSITIVE)).asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }


    public void createTestData() {
        for (int i = 0; i < 100; i++) {
            String v;
            if (i % 2 == 0) {
                v = "VALUE " + (i + 1);
            } else {
                v = "value " + (i + 1);
            }
            UncachedObject o = new UncachedObject(v, i);
            morphium.store(o);
        }
    }
}
