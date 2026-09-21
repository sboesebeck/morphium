package de.caluga.test.mongo.suite.inmem;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bson.MongoMaxKey;
import de.caluga.morphium.driver.bson.MongoMinKey;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Every value type the BSON decoder can put into the store must come out of a dump file as
 * parseable JSON again. The dump writer wrote anything it did not know as a bare
 * {@code toString()} token: a {@code java.util.regex.Pattern} became {@code "pattern_value" : a*},
 * which no parser accepts. One such document in one database was enough to fail the restore of
 * that database on every node of a replica set at once (all three hold the same dump), and the
 * partial-restore guard (#306) then kept all three from standing for election: no primary until
 * the file was removed by hand. Seen on the test runner on 2026-09-21 after a rolling restart,
 * caused by the regex field of the wire-type test.
 */
@Tag("inmemory")
public class InMemDumpValueTypesTest {

    private static final String DB = "dump_value_types";

    private static Map<String, Object> roundTrip(Map<String, Object> doc) throws Exception {
        InMemoryDriver src = new InMemoryDriver();
        Map<String, List<Map<String, Object>>> db = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(doc);
        db.put("coll", docs);
        src.setDatabase(DB, db);

        File dir = Files.createTempDirectory("dump-value-types").toFile();
        src.dumpToFile(DB, new File(dir, DB + ".morphium.gz"));

        InMemoryDriver dst = new InMemoryDriver();
        InMemoryDriver.DirectoryRestoreResult result = dst.restoreAllFromDirectoryResult(dir);
        assertThat(result.isComplete())
            .as("the dump of " + doc + " must be parseable again - failed files: " + result.getFailedFiles())
            .isTrue();
        List<Map<String, Object>> restored = dst.getDatabase(DB).get("coll");
        assertThat(restored).hasSize(1);
        return restored.get(0);
    }

    @Test
    public void aRegexValueSurvivesDumpAndRestore() throws Exception {
        Pattern p = Pattern.compile("a*\"b", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Map<String, Object> back = roundTrip(Doc.of("_id", "regex", "pattern_value", p));

        assertThat(back.get("pattern_value")).as("a regex must restore as a regex").isInstanceOf(Pattern.class);
        Pattern q = (Pattern) back.get("pattern_value");
        assertThat(q.pattern()).isEqualTo(p.pattern());
        assertThat(q.flags()).as("flags are part of the value").isEqualTo(p.flags());
    }

    @Test
    public void aTimestampValueSurvivesDumpAndRestore() throws Exception {
        MongoTimestamp ts = new MongoTimestamp(1700000000, 7);
        Map<String, Object> back = roundTrip(Doc.of("_id", "ts", "ts_value", ts));

        assertThat(back.get("ts_value")).isInstanceOf(MongoTimestamp.class);
        assertThat(((MongoTimestamp) back.get("ts_value")).getValue()).isEqualTo(ts.getValue());
    }

    @Test
    public void theEstablishedWireTypesStillRoundTrip() throws Exception {
        // Guards the dump shape now that the PoppyDB dump path no longer runs the entity mapper.
        MorphiumId id = new MorphiumId();
        Date date = new Date(1700000000123L);
        UUID uuid = UUID.randomUUID();
        byte[] bytes = new byte[] {1, 2, 3, -1};
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", id);
        doc.put("date", date);
        doc.put("uuid", uuid);
        doc.put("bytes", bytes);
        doc.put("text", "quote\"and\\backslash");
        doc.put("n", 42L);
        doc.put("d", 1.5);
        doc.put("b", true);
        doc.put("dec", new BigDecimal("12.34"));
        doc.put("nested", Doc.of("inner", date));
        doc.put("list", List.of(uuid, 7));
        Map<String, Object> back = roundTrip(doc);

        assertThat(back.get("_id")).isEqualTo(id);
        assertThat(back.get("date")).isEqualTo(date);
        assertThat(back.get("uuid")).isEqualTo(uuid);
        assertThat((byte[]) back.get("bytes")).containsExactly(1, 2, 3, -1);
        assertThat(back.get("text")).isEqualTo("quote\"and\\backslash");
        assertThat(((Number) back.get("n")).longValue()).isEqualTo(42L);
        assertThat(((Number) back.get("d")).doubleValue()).isEqualTo(1.5);
        assertThat(back.get("b")).isEqualTo(true);
        assertThat(((Number) back.get("dec")).doubleValue()).isEqualTo(12.34);
        assertThat(((Map<?, ?>) back.get("nested")).get("inner")).isEqualTo(date);
        assertThat(((List<?>) back.get("list")).get(0)).isEqualTo(uuid);
    }

    @Test
    public void unknownValueTypesNeverBreakTheDump() throws Exception {
        // Types the writer has no marker for: they may lose their type, but never the whole database.
        Map<String, Object> back = roundTrip(Doc.of("_id", "odd",
            "min", new MongoMinKey(), "max", new MongoMaxKey(),
            "nan", Double.NaN, "inf", Double.POSITIVE_INFINITY));

        assertThat(back).containsKeys("min", "max", "nan", "inf");
    }
}
