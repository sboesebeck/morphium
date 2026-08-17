package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #306 (follow-up): after the restore loop was made fault tolerant, the
 * customer ACC rollout revealed that every NON-EMPTY database failed to restore with
 * "RuntimeException: Parsing failed". Root cause: the dump JSON writer ({@code Utils.writeJson})
 * did not escape strings at all (quotes, backslashes, control characters broke the JSON) and
 * wrote {@code Date}/{@code UUID} values as bare unquoted {@code toString()} tokens - so any
 * database containing real data (dates, German text with quotes, ...) produced a dump that
 * could never be parsed again. On top, dump/restore used the platform default charset, so a
 * dump written on one JVM could be silently mojibake'd on another.
 */
@Tag("inmemory")
public class InMemDumpRoundtripTest {

    private static final String DB = "roundtrip_db";
    private static final String COLL = "test_coll";

    /** Dump the given docs with one driver, restore them with a fresh one, return the restored docs. */
    private List<Map<String, Object>> roundtrip(Path tmp, List<Map<String, Object>> docs) throws Exception {
        InMemoryDriver src = new InMemoryDriver();
        Map<String, List<Map<String, Object>>> db = new HashMap<>();
        db.put(COLL, docs);
        src.setDatabase(DB, db);
        File f = new File(tmp.toFile(), DB + ".morphium.gz");
        src.dumpToFile(DB, f);

        InMemoryDriver target = new InMemoryDriver();
        target.restoreFromFile(f);
        List<Map<String, Object>> restored = target.getDatabase(DB).get(COLL);
        assertNotNull(restored, "restored database must contain the collection");
        assertEquals(docs.size(), restored.size(), "all documents must survive the roundtrip");
        return restored;
    }

    private static Map<String, Object> byId(List<Map<String, Object>> docs, Object id) {
        for (Map<String, Object> d : docs) {
            if (String.valueOf(d.get("_id")).equals(String.valueOf(id))) {
                return d;
            }
        }
        throw new AssertionError("no document with _id " + id + " in " + docs);
    }

    @Test
    public void umlautsQuotesAndControlCharsSurviveRoundtrip(@TempDir Path tmp) throws Exception {
        String german = "Größenwahn beim Fußball: „Übermäßige Ärgernisse“ – köstlich!";
        String quotes = "he said \"hello\" and left a backslash: C:\\temp\\x and a slash: a/b";
        String newlines = "line one\nline two\r\nline three\ttabbed";
        String nonBmp = "emoji \uD83D\uDE00 and čçñøπ€";
        StringBuilder big = new StringBuilder();
        while (big.length() < 1_000_000) {
            big.append("Die „Süddeutsche“ meldet: Überraschung an der Börse — 42 % plus!\n");
        }

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", "german", "text", german));
        docs.add(Doc.of("_id", "quotes", "text", quotes));
        docs.add(Doc.of("_id", "newlines", "text", newlines));
        docs.add(Doc.of("_id", "nonbmp", "text", nonBmp));
        docs.add(Doc.of("_id", "big", "text", big.toString()));

        List<Map<String, Object>> restored = roundtrip(tmp, docs);

        assertEquals(german, byId(restored, "german").get("text"));
        assertEquals(quotes, byId(restored, "quotes").get("text"));
        assertEquals(newlines, byId(restored, "newlines").get("text"));
        assertEquals(nonBmp, byId(restored, "nonbmp").get("text"));
        assertEquals(big.toString(), byId(restored, "big").get("text"));
    }

    @Test
    public void datesIdsAndUuidsSurviveRoundtrip(@TempDir Path tmp) throws Exception {
        Date date = new Date(1723891234567L);
        MorphiumId mid = new MorphiumId();
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", mid, "created", date, "uuid", uuid, "num", 42L));

        List<Map<String, Object>> restored = roundtrip(tmp, docs);
        Map<String, Object> doc = restored.get(0);

        assertInstanceOf(Date.class, doc.get("created"), "Date must be restored as Date, got: " + doc.get("created"));
        assertEquals(date, doc.get("created"));
        assertInstanceOf(UUID.class, doc.get("uuid"), "UUID must be restored as UUID, got: " + doc.get("uuid"));
        assertEquals(uuid, doc.get("uuid"));
        // the wire protocol stores ids as MorphiumId - a restore must not silently turn them
        // into Strings, or clients can no longer find/delete their documents by id
        assertInstanceOf(MorphiumId.class, doc.get("_id"),
                "_id must be restored as MorphiumId, got: " + doc.get("_id").getClass());
        assertEquals(mid, doc.get("_id"));
        assertEquals(42L, doc.get("num"));
    }

    @Test
    public void numericTypesKeepTheirTypeAndPrecision(@TempDir Path tmp) throws Exception {
        // JSON has one number type, Java and BSON do not. A roundtrip that silently turns a
        // Long into an Integer breaks every later equality check and range query on that field,
        // and a truncated double loses data outright - both are the kind of damage that only
        // shows up long after the restore.
        Map<String, Object> numbers = new java.util.LinkedHashMap<>();
        numbers.put("_id", "numbers");
        numbers.put("anInt", 42);
        numbers.put("aLong", 9_000_000_000L);
        numbers.put("aDouble", 3.14159265358979);
        numbers.put("negative", -17);
        numbers.put("zero", 0);
        numbers.put("maxLong", Long.MAX_VALUE);
        numbers.put("aBool", true);
        numbers.put("aFalse", false);

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(numbers);

        Map<String, Object> doc = byId(roundtrip(tmp, docs), "numbers");

        // JSON has a single number type, so integral values come back as Long - that is lossless
        // and expected. What must NOT happen is the other direction: a Long narrowed to an
        // Integer would overflow anything above 2^31, and a truncated double loses data.
        assertEquals(42L, ((Number) doc.get("anInt")).longValue());
        assertEquals(9_000_000_000L, doc.get("aLong"));
        assertInstanceOf(Long.class, doc.get("aLong"),
                "a value beyond the Integer range must not be narrowed, got: "
                        + doc.get("aLong").getClass());
        assertEquals(3.14159265358979, (Double) doc.get("aDouble"), 0.0);
        assertInstanceOf(Double.class, doc.get("aDouble"),
                "a floating point value must stay floating point, got: " + doc.get("aDouble").getClass());
        assertEquals(-17L, ((Number) doc.get("negative")).longValue());
        assertEquals(0L, ((Number) doc.get("zero")).longValue());
        assertEquals(Long.MAX_VALUE, doc.get("maxLong"));
        assertEquals(true, doc.get("aBool"));
        assertEquals(false, doc.get("aFalse"));
    }

    @Test
    public void nullsAndEmptyContainersSurviveRoundtrip(@TempDir Path tmp) throws Exception {
        // An explicit null is not the same as an absent field, and an empty list is not the
        // same as null - messaging relies on exactly that distinction (processed_by starts as
        // an empty list).
        Map<String, Object> doc = new java.util.LinkedHashMap<>();
        doc.put("_id", "empties");
        doc.put("explicitNull", null);
        doc.put("emptyList", new ArrayList<>());
        doc.put("emptyMap", new java.util.LinkedHashMap<>());
        doc.put("emptyString", "");

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(doc);

        Map<String, Object> restored = byId(roundtrip(tmp, docs), "empties");

        assertTrue(restored.containsKey("explicitNull"), "an explicit null must not vanish from the document");
        assertNull(restored.get("explicitNull"));
        assertEquals(new ArrayList<>(), restored.get("emptyList"));
        assertEquals(new java.util.LinkedHashMap<>(), restored.get("emptyMap"));
        assertEquals("", restored.get("emptyString"));
    }

    @Test
    public void supplementaryPlaneCharactersSurviveRoundtrip(@TempDir Path tmp) throws Exception {
        // Emoji are surrogate pairs in Java and four bytes in UTF-8 - the case where a
        // char-wise escaper or a wrong charset shows up as a mangled or truncated string.
        String text = "Nachricht \uD83D\uDE80 mit Emoji und \u00dcmlauten \u2013 plus Gedankenstrich";

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", "emoji", "text", text));

        assertEquals(text, byId(roundtrip(tmp, docs), "emoji").get("text"));
    }

    @Test
    public void binaryDataSurvivesRoundtrip(@TempDir Path tmp) throws Exception {
        byte[] bin = new byte[256];
        for (int i = 0; i < bin.length; i++) {
            bin[i] = (byte) i;
        }

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", "bin", "payload", bin));

        List<Map<String, Object>> restored = roundtrip(tmp, docs);
        Object payload = byId(restored, "bin").get("payload");
        assertInstanceOf(byte[].class, payload, "byte[] must be restored as byte[], got: "
                + (payload == null ? "null" : payload.getClass()));
        assertArrayEquals(bin, (byte[]) payload);
    }

    @Test
    public void nestedStructuresSurviveRoundtrip(@TempDir Path tmp) throws Exception {
        Map<String, Object> inner = Doc.of("titel", "Über \"alles\"", "wann", new Date(1723891234567L));
        List<Object> list = new ArrayList<>();
        list.add("Ärger\nmit Zeilenumbruch");
        list.add(Doc.of("tief", Doc.of("noch tiefer", "größer")));
        list.add(7L);

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", "nested", "inner", inner, "list", list));

        List<Map<String, Object>> restored = roundtrip(tmp, docs);
        Map<String, Object> doc = byId(restored, "nested");

        Map<String, Object> restoredInner = (Map<String, Object>) doc.get("inner");
        assertEquals("Über \"alles\"", restoredInner.get("titel"));
        assertEquals(new Date(1723891234567L), restoredInner.get("wann"));

        List<Object> restoredList = (List<Object>) doc.get("list");
        assertEquals("Ärger\nmit Zeilenumbruch", restoredList.get(0));
        Map<String, Object> tief = (Map<String, Object>) restoredList.get(1);
        assertEquals("größer", ((Map<String, Object>) tief.get("tief")).get("noch tiefer"));
        assertEquals(7L, restoredList.get(2));
    }

    /** Write an old-format dump by hand, in the given charset, the way the pre-fix writer did. */
    private static File writeLegacyDump(Path tmp, String json, Charset cs) throws Exception {
        File f = new File(tmp.toFile(), "legacy.morphium.gz");
        try (FileOutputStream fos = new FileOutputStream(f);
            GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write(json.getBytes(cs));
        }
        return f;
    }

    /**
     * Cross-environment case: a dump written by a JVM whose platform default charset was
     * ISO-8859-1 (old writer used {@code new OutputStreamWriter(gzip)} without a charset) must
     * still restore correctly - the bytes are not valid UTF-8 for every umlaut.
     */
    @Test
    public void legacyLatin1DumpIsStillReadable(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"legacy_db\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"doc1\", \"text\" : \"M\u00fcller \u00e4\u00f6\u00fc\u00df\" } ] } }";
        File f = writeLegacyDump(tmp, json, StandardCharsets.ISO_8859_1);

        InMemoryDriver target = new InMemoryDriver();
        target.restoreFromFile(f);

        Object text = target.getDatabase("legacy_db").get("coll").get(0).get("text");
        assertEquals("M\u00fcller \u00e4\u00f6\u00fc\u00df", text,
                "a legacy latin-1 dump must not be mojibake'd on restore");
    }

    /**
     * Legacy dumps could contain RAW newlines inside JSON strings (the old writer did not
     * escape anything). The old reader joined lines with readLine(), silently deleting those
     * newlines from the data. The new reader must preserve them.
     */
    @Test
    public void legacyDumpWithRawNewlineInStringKeepsTheNewline(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"legacy_nl\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"doc1\", \"text\" : \"zeile eins\nzeile zwei\" } ] } }";
        File f = writeLegacyDump(tmp, json, StandardCharsets.UTF_8);

        InMemoryDriver target = new InMemoryDriver();
        target.restoreFromFile(f);

        Object text = target.getDatabase("legacy_nl").get("coll").get(0).get("text");
        assertEquals("zeile eins\nzeile zwei", text,
                "raw newlines inside legacy dump strings must survive the restore");
    }

    /**
     * "Parsing failed" without any detail cost us a day during the #306 incident: the wrapped
     * cause (with json-simple's error position) must be part of the message, because many log
     * statements only print getMessage().
     */
    @Test
    public void parseFailureMessageNamesTheActualCause() {
        ObjectMapperImpl mapper = new ObjectMapperImpl();
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> mapper.deserialize(Map.class, "{ \"broken\" : ]]"));
        assertNotNull(ex.getCause(), "original cause must stay attached");
        assertNotEquals("Parsing failed", ex.getMessage(),
                "the message must carry the cause, not just 'Parsing failed'");
        assertTrue(ex.getMessage().contains(ex.getCause().getClass().getSimpleName())
                        || ex.getMessage().contains(String.valueOf(ex.getCause().getMessage()))
                        || ex.getMessage().toLowerCase().contains("position"),
                "message must describe the underlying parse error, got: " + ex.getMessage());
    }
}
