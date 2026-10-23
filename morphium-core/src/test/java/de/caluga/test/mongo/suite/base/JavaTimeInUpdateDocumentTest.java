package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MOR-006 (spike): java.time values and Date in the operator maps of {@code findOneAndUpdate} and {@code Query.set} must read back as the
 * same value and be stored in the same shape as {@code store()} writes them, on a real MongoDB and on the InMemoryDriver.
 */
@Tag("core")
public class JavaTimeInUpdateDocumentTest extends MultiDriverTestBase {

    @Entity(translateCamelCase = false)
    public static class Timed {
        @Id
        public MorphiumId id;
        public String key;
        public Instant instant;
        public LocalDateTime localDateTime;
        public LocalDate localDate;
        public LocalTime localTime;
        public Date date;
    }

    private static final Instant INSTANT = Instant.parse("2026-10-15T12:34:56.789Z");
    private static final LocalDateTime LDT = LocalDateTime.of(2026, 10, 15, 12, 34, 56, 789_000_000);
    private static final LocalDate LD = LocalDate.of(2026, 10, 15);
    private static final LocalTime LT = LocalTime.of(12, 34, 56, 789_000_000);
    private static final Date DATE = new Date(1_792_000_000_000L);

    private Map<String, Object> raw(Morphium m, String key) throws Exception {
        Query<Timed> q = m.createQueryFor(Timed.class).f("key").eq(key);
        FindCommand cmd = q.getFindCmd();

        try {
            cmd.setProjection(new Doc());
            List<Map<String, Object>> docs = cmd.execute();
            assertEquals(1, docs.size(), String.valueOf(docs));
            return docs.get(0);
        } finally {
            cmd.releaseConnection();
        }
    }

    private void assertAll(Timed t) {
        assertEquals(INSTANT, t.instant);
        assertEquals(LDT, t.localDateTime);
        assertEquals(LD, t.localDate);
        assertEquals(LT, t.localTime);
        assertEquals(DATE, t.date);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateReadsBackJavaTimeValues(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> set = new java.util.HashMap<>();
            set.put("key", "u");
            set.put("instant", INSTANT);
            set.put("localDateTime", LDT);
            set.put("localDate", LD);
            set.put("localTime", LT);
            set.put("date", DATE);
            Timed returned = m.createQueryFor(Timed.class).f("key").eq("u").findOneAndUpdate(UtilsMap.of("$set", set), true, true);
            assertAll(returned);
            assertAll(m.createQueryFor(Timed.class).f("key").eq("u").get());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void querySetReadsBackJavaTimeValues(Morphium m) throws Exception {
        try (m) {
            Timed t = new Timed();
            t.key = "s";
            m.store(t);
            Map<String, Object> set = new java.util.HashMap<>();
            set.put("instant", INSTANT);
            set.put("localDateTime", LDT);
            set.put("localDate", LD);
            set.put("localTime", LT);
            set.put("date", DATE);
            m.createQueryFor(Timed.class).f("key").eq("s").set(set, false, false);
            assertAll(m.createQueryFor(Timed.class).f("key").eq("s").get());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateAndStoreWriteTheSameShape(Morphium m) throws Exception {
        try (m) {
            Timed stored = new Timed();
            stored.key = "stored";
            stored.instant = INSTANT;
            stored.localDateTime = LDT;
            stored.localDate = LD;
            stored.localTime = LT;
            stored.date = DATE;
            m.store(stored);

            Map<String, Object> set = new java.util.HashMap<>();
            set.put("key", "updated");
            set.put("instant", INSTANT);
            set.put("localDateTime", LDT);
            set.put("localDate", LD);
            set.put("localTime", LT);
            set.put("date", DATE);
            m.createQueryFor(Timed.class).f("key").eq("updated").findOneAndUpdate(UtilsMap.of("$set", set), true, true);

            Map<String, Object> a = raw(m, "stored");
            Map<String, Object> b = raw(m, "updated");

            for (String f : List.of("instant", "localDateTime", "localDate", "localTime", "date")) {
                assertEquals(String.valueOf(a.get(f)), String.valueOf(b.get(f)), "stored shape of " + f + " differs between store() and the update");
            }
        }
    }
}
