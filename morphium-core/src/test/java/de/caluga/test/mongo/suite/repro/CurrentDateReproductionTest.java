package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.repro.ReproSupport.Outcome;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduction of hypothesis 3 of the Morphium update-path research (docs/research/reproduction-results.md): {@code MorphiumBase.currentDate(...)}
 * delegates to {@code set(query, {"$currentDate": {field: 1}}, ...)}, i.e. the {@code $currentDate} operator
 * ends up INSIDE a {@code $set}: {@code {"$set": {"$currentDate": {"lastSeen": 1}}}} - not a valid
 * {@code $currentDate} update.
 * <p>
 * Fixed by MOR-004: {@code Query.set} (and so {@code currentDate}) treats a key that starts with {@code $} and has a map as its value
 * as an update operator of its own, not as a field of {@code $set}. The class still documents the original defect: the update
 * document was {@code {"$set": {"$currentDate": ...}}}, a real MongoDB answered with code 52 inside the returned map (no exception) and the
 * InMemoryDriver reported {@code n=1, nModified=1} without changing anything.
 * Controls: a raw {@code $currentDate} through {@code findOneAndUpdate} and the bulk-context variant, which
 * build a real {@code $currentDate} update document.
 */
@Tag("core")
public class CurrentDateReproductionTest extends MultiDriverTestBase {

    public enum Fld {
        lastSeen
    }

    @Entity(translateCamelCase = false)
    public static class Clocked {
        @Id
        public MorphiumId id;
        public String name;
        public Date lastSeen;
    }

    private static boolean isInMem(Morphium m) {
        return m.getDriver() instanceof InMemoryDriver;
    }

    private Date seed(Morphium m) {
        Clocked c = new Clocked();
        c.name = "n";
        c.lastSeen = new Date(0);
        m.store(c);
        return c.lastSeen;
    }

    private Query<Clocked> byName(Morphium m) {
        return m.createQueryFor(Clocked.class).f("name").eq("n");
    }

    private Map<String, Object> raw(Morphium m) {
        List<Map<String, Object>> docs = m.createQueryFor(Clocked.class).asMapList();
        assertEquals(1, docs.size());
        return docs.get(0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void currentDateByFieldName(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            seed(morphium);
            Outcome<Map<String, Object>> o = ReproSupport.attempt(() -> morphium.currentDate(byName(morphium), "lastSeen", false, false));
            ReproSupport.report(driver, "H3 currentDate(String)", o.describe());
            Map<String, Object> raw = raw(morphium);
            ReproSupport.report(driver, "H3 currentDate(String) raw document afterwards", String.valueOf(raw));

            assertFalse(o.failed(), o.describe());
            assertTrue(((Date) raw.get("lastSeen")).getTime() > 1_000_000_000_000L, "lastSeen was refreshed: " + raw);
            assertFalse(raw.containsKey("$currentDate"), "no field named $currentDate was stored: " + raw);
            assertEquals(1, ((Number) o.value.get("n")).intValue(), String.valueOf(o.value));
            assertEquals(1, ((Number) o.value.get("nModified")).intValue(), String.valueOf(o.value));
            assertNull(o.value.get("writeErrors"), String.valueOf(o.value));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void currentDateByEnumAndWithUpsert(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            seed(morphium);
            Outcome<Map<String, Object>> o = ReproSupport.attempt(() -> morphium.currentDate(byName(morphium), Fld.lastSeen, true, false));
            ReproSupport.report(driver, "H3 currentDate(Enum, upsert=true)", o.describe());
            assertFalse(o.failed(), o.describe());
            assertNull(o.value.get("writeErrors"), String.valueOf(o.value));
            List<Map<String, Object>> docs = morphium.createQueryFor(Clocked.class).asMapList();
            assertEquals(1, docs.size(), "the existing document matches, nothing is inserted: " + docs);
            assertTrue(((Date) docs.get(0).get("lastSeen")).getTime() > 1_000_000_000_000L, String.valueOf(docs));

            // upsert inserts when nothing matches, with the current date and the equality field of the filter
            Outcome<Map<String, Object>> ins = ReproSupport.attempt(() -> morphium.currentDate(morphium.createQueryFor(Clocked.class).f("name").eq("fresh"), Fld.lastSeen, true, false));
            assertFalse(ins.failed(), ins.describe());
            List<Map<String, Object>> after = morphium.createQueryFor(Clocked.class).f("name").eq("fresh").asMapList();
            assertEquals(1, after.size(), String.valueOf(after));
            assertTrue(((Date) after.get(0).get("lastSeen")).getTime() > 1_000_000_000_000L, String.valueOf(after));
        }
    }

    /** MOR-004: operator entries in the map of Query.set are real operators and can be mixed with plain fields. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void operatorKeysInQuerySetAreRealOperatorsAndMixWithPlainFields(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium);
            Map<String, Object> values = new java.util.HashMap<>();
            values.put("name", "renamed");
            values.put("$currentDate", UtilsMap.of("lastSeen", true));
            Outcome<Map<String, Object>> o = ReproSupport.attempt(() -> byName(morphium).set(values, false, false));
            assertFalse(o.failed(), o.describe());
            assertNull(o.value.get("writeErrors"), String.valueOf(o.value));
            Map<String, Object> raw = raw(morphium);
            assertEquals("renamed", raw.get("name"));
            assertTrue(((Date) raw.get("lastSeen")).getTime() > 1_000_000_000_000L, String.valueOf(raw));
            assertFalse(raw.containsKey("$currentDate"), String.valueOf(raw));

            // only operators, no plain field: no empty $set may be sent
            Outcome<Map<String, Object>> only = ReproSupport.attempt(() -> morphium.createQueryFor(Clocked.class).f("name").eq("renamed").set(Map.of("$currentDate", UtilsMap.of("lastSeen", true)), false, false));
            assertFalse(only.failed(), only.describe());
            assertNull(only.value.get("writeErrors"), String.valueOf(only.value));
        }
    }

    /** Fidelity: a $-prefixed field name under $set is rejected with code 52 by a real MongoDB and now by the InMemoryDriver, too. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDollarPrefixedFieldUnderSetIsRejectedOnBothDrivers(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            seed(morphium);
            Outcome<Map<String, Object>> o = ReproSupport.attempt(() -> byName(morphium).set(Map.of("$weird", 1), false, false));
            ReproSupport.report(driver, "H3 $weird under $set", o.describe());
            assertFalse(o.failed(), "the error only shows up inside the returned map: " + o.describe());
            if (!isInMem(morphium)) {
                // a real MongoDB reports n=0, the InMemoryDriver still counts the matched document
                assertEquals(0, ((Number) o.value.get("n")).intValue(), String.valueOf(o.value));
            }

            assertEquals(0, ((Number) o.value.get("nModified")).intValue(), String.valueOf(o.value));
            List<?> writeErrors = (List<?>) o.value.get("writeErrors");
            assertNotNull(writeErrors, "writeErrors expected: " + o.value);
            assertEquals(52, ((Number) ((Map<?, ?>) writeErrors.get(0)).get("code")).intValue());
            assertFalse(raw(morphium).containsKey("$weird"));
        }
    }

    /** Control 1: a hand-written $currentDate update through findOneAndUpdate is valid on both drivers. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void rawCurrentDateControl(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            seed(morphium);
            Outcome<Clocked> o = ReproSupport.attempt(() -> byName(morphium).findOneAndUpdate(UtilsMap.of("$currentDate", UtilsMap.of("lastSeen", true)), false, true));
            ReproSupport.report(driver, "H3 control raw $currentDate via findOneAndUpdate", o.describe());
            assertFalse(o.failed(), o.describe());
            assertTrue(o.value.lastSeen.getTime() > 1_000_000_000_000L, "lastSeen refreshed: " + o.value.lastSeen);
            assertFalse(raw(morphium).containsKey("$currentDate"));
        }
    }

    /** Control 2: the bulk context builds a proper $currentDate update. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void bulkCurrentDateControl(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            seed(morphium);
            MorphiumBulkContext<Clocked> ctx = morphium.createBulkRequestContext(Clocked.class, false);
            ctx.addCurrentDateRequest(byName(morphium), false, false, "lastSeen");
            Outcome<Map<String, Object>> o = ReproSupport.attempt(ctx::runBulk);
            ReproSupport.report(driver, "H3 control bulk addCurrentDateRequest", o.describe());
            assertFalse(o.failed(), o.describe());
            Map<String, Object> raw = raw(morphium);
            ReproSupport.report(driver, "H3 control bulk raw document", String.valueOf(raw));
            assertTrue(((Date) raw.get("lastSeen")).getTime() > 1_000_000_000_000L);
            assertFalse(raw.containsKey("$currentDate"));
        }
    }
}
