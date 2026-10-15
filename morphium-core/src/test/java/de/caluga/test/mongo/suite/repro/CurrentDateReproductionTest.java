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
 * The tests assert the CURRENT behaviour per driver: what a real MongoDB answers, and what the InMemoryDriver
 * does with the very same document (it may silently store a field literally named "$currentDate").
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

            assertFalse(o.failed(), "the call returns normally on both drivers: " + o.describe());
            assertEquals(new Date(0), raw.get("lastSeen"), "lastSeen was NOT refreshed");
            assertFalse(raw.containsKey("$currentDate"), "no field named $currentDate was stored: " + raw);

            if (isInMem(morphium)) {
                // InMemoryDriver: reports success (n=1, nModified=1) although nothing was changed
                assertEquals(1, ((Number) o.value.get("n")).intValue());
                assertEquals(1, ((Number) o.value.get("nModified")).intValue());
                assertNull(o.value.get("writeErrors"));
            } else {
                // real MongoDB: the server rejects the document with code 52, but the error only shows up
                // inside the returned map (writeErrors); no exception is thrown
                assertEquals(0, ((Number) o.value.get("n")).intValue());
                assertEquals(0, ((Number) o.value.get("nModified")).intValue());
                List<?> writeErrors = (List<?>) o.value.get("writeErrors");
                assertNotNull(writeErrors, "writeErrors expected in the returned map: " + o.value);
                Map<?, ?> we = (Map<?, ?>) writeErrors.get(0);
                assertEquals(52, ((Number) we.get("code")).intValue());
                assertTrue(String.valueOf(we.get("errmsg")).contains("dollar ($) prefixed field '$currentDate'"), String.valueOf(we));
            }
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
            ReproSupport.report(driver, "H3 currentDate(Enum, upsert=true) raw documents afterwards",
                                String.valueOf(morphium.createQueryFor(Clocked.class).asMapList()));
            assertFalse(o.failed(), o.describe());
            List<Map<String, Object>> docs = morphium.createQueryFor(Clocked.class).asMapList();
            assertEquals(1, docs.size(), "upsert=true must not insert anything either: " + docs);
            assertEquals(new Date(0), docs.get(0).get("lastSeen"));
            if (!isInMem(morphium)) {
                assertNotNull(o.value.get("writeErrors"), String.valueOf(o.value));
            }
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
