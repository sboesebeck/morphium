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
        Query<UncachedObject> waitQuery = morphium.createQueryFor(UncachedObject.class);
        waitQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", () -> waitQuery.countAll() == 100);

        Query<UncachedObject> uppercaseQuery = morphium.createQueryFor(UncachedObject.class);
        uppercaseQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> uppercaseMatches = uppercaseQuery.f(UncachedObject.Fields.strValue).matches("VALUE.*").asList();
        assertEquals(50, uppercaseMatches.size());

        Query<UncachedObject> endingNineQuery = morphium.createQueryFor(UncachedObject.class);
        endingNineQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> lst = endingNineQuery.f(UncachedObject.Fields.strValue).matches("9$").asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue (o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternRegexTest() throws Exception {
        createTestData();
        Query<UncachedObject> waitQuery = morphium.createQueryFor(UncachedObject.class);
        waitQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->waitQuery.countAll() == 100);

        Query<UncachedObject> uppercaseQuery = morphium.createQueryFor(UncachedObject.class);
        uppercaseQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> uppercaseMatches = uppercaseQuery.f(UncachedObject.Fields.strValue).matches(Pattern.compile("VALUE.*")).asList();
        assertEquals(50, uppercaseMatches.size());

        Query<UncachedObject> endingNineQuery = morphium.createQueryFor(UncachedObject.class);
        endingNineQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> lst = endingNineQuery.f(UncachedObject.Fields.strValue).matches(Pattern.compile("9$")).asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void regexOptionTest() throws Exception {
        createTestData();
        Query<UncachedObject> waitQuery = morphium.createQueryFor(UncachedObject.class);
        waitQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->waitQuery.countAll() == 100);

        Query<UncachedObject> exactMatchQuery = morphium.createQueryFor(UncachedObject.class);
        exactMatchQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> exactMatch = exactMatchQuery.f(UncachedObject.Fields.strValue).matches("value 9$", "i").asList();
        assertEquals(1, exactMatch.size());
        Query<UncachedObject> regexQuery = morphium.createQueryFor(UncachedObject.class);
        regexQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> lst = regexQuery.f(UncachedObject.Fields.strValue).matches("value .*9$", "i").asList();
        assertTrue(lst.size() > 1);
        for (UncachedObject o : lst) {
            assertTrue(o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternOptionTest() throws Exception {
        createTestData();
        Query<UncachedObject> waitQuery = morphium.createQueryFor(UncachedObject.class);
        waitQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        TestUtils.waitForConditionToBecomeTrue(1500, "Writes failed??!?", ()->waitQuery.countAll() == 100);

        Query<UncachedObject> exactMatchQuery = morphium.createQueryFor(UncachedObject.class);
        exactMatchQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> exactMatch = exactMatchQuery.f(UncachedObject.Fields.strValue)
                                                        .matches(Pattern.compile("value 9$", Pattern.CASE_INSENSITIVE))
                                                        .asList();
        assertEquals(1, exactMatch.size());
        Query<UncachedObject> regexQuery = morphium.createQueryFor(UncachedObject.class);
        regexQuery.setReadPreferenceLevel(de.caluga.morphium.annotations.ReadPreferenceLevel.PRIMARY);
        List<UncachedObject> lst = regexQuery.f(UncachedObject.Fields.strValue).matches(Pattern.compile("value .*9$", Pattern.CASE_INSENSITIVE)).asList();
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
