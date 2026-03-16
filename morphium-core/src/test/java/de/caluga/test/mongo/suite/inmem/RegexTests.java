package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("inmemory")
public class RegexTests extends MorphiumInMemTestBase {

    @Test
    public void simpleRegexTests() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assertEquals(50, q.f(UncachedObject.Fields.strValue).matches("VALUE.*").countAll());
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches("9$").asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternRegexTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assertEquals(50, q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("VALUE.*")).countAll());
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
        assertEquals(1, q.f(UncachedObject.Fields.strValue).matches("value 9$", "i").countAll());
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
        assertEquals(1, q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("value 9$", Pattern.CASE_INSENSITIVE)).countAll());
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches(Pattern.compile("value .*9$", Pattern.CASE_INSENSITIVE)).asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void startEndTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assertEquals(2, q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("^value [12]3$", Pattern.CASE_INSENSITIVE)).countAll());
        q = morphium.createQueryFor(UncachedObject.class);
        assertEquals(2, q.f(UncachedObject.Fields.strValue).matches("^value [12]3$", "i").countAll());
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
