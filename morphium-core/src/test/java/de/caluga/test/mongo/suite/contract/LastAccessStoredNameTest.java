package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.LastAccess;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * MOR-052: the access time is written in the type of the field. It was written as a number whatever the field was: a {@code Date} field held a number in the document and a
 * {@code String} field with a date format too, while the entity that was read carried the date and the text. The name is the stored name (the ledger said the Java name).
 */
@Tag("core")
public class LastAccessStoredNameTest extends MultiDriverTestBase {

    @Entity
    @NoCache
    @LastAccess // the class carries the switch, the fields say where the time goes
    public static class Seen {
        @Id
        public String id;
        public String value;
        @LastAccess
        public Date lastSeenAt;
        @LastAccess
        public long accessMillis;
        @LastAccess(dateFormat = "yyyy-MM-dd HH:mm")
        public String accessText;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theAccessTimeIsStoredUnderTheStoredNameInTheTypeOfTheField(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Seen.class);
            Seen s = new Seen();
            s.id = "a";
            s.value = "v";
            m.store(s);

            Seen read = m.createQueryFor(Seen.class).f("id").eq("a").get();
            assertNotNull(read);
            assertNotNull(read.lastSeenAt, "the entity that was read carries the access time");

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Seen.class).f("id").eq("a")).get(0);
            assertFalse(raw.containsKey("lastSeenAt"), "the Java name is no field of the document: " + raw);
            assertFalse(raw.containsKey("accessMillis"), "the Java name is no field of the document: " + raw);
            assertInstanceOf(Date.class, raw.get("last_seen_at"), "a Date field holds a date: " + raw);
            assertInstanceOf(Number.class, raw.get("access_millis"), "a long field holds a number: " + raw);
            assertInstanceOf(String.class, raw.get("access_text"), "a String field holds the formatted time: " + raw);
            assertEquals(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date()).substring(0, 10), ((String) raw.get("access_text")).substring(0, 10));
            assertEquals(((Date) raw.get("last_seen_at")).getTime(), ((Number) raw.get("access_millis")).longValue(), 5000, "both are the time of the read");
        }
    }
}
