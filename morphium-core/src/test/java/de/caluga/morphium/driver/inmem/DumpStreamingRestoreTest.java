package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for #366: the restore must read a dump incrementally, not hold it in memory as
 * one {@code byte[]} and one {@code String} first.
 *
 * <p>Those two buffers were what capped a restorable database at 2^30 characters - past it the
 * restore died with {@code OutOfMemoryError: UTF16 String size is ...} regardless of heap, on a
 * file the write side had produced without complaint. Proving that the cap is gone must not take a
 * gigabyte-sized dump: a stream that refuses to be slurped, and a read granularity small enough
 * that any dump spans many refills, show the same thing in milliseconds.
 */
@Tag("inmemory")
public class DumpStreamingRestoreTest {
    private final String db = "streamingdb";
    private final String coll = "streamingcoll";

    private final Date date = new Date(1723891234567L);
    private final UUID uuid = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
    private final MorphiumId id = new MorphiumId();
    private final byte[] binary = {1, 2, 3, 4, 5, 0, -1, -128, 127};

    /**
     * A database whose dump is comfortably larger than the chunk sizes exercised below, with the
     * marked types (Date/UUID/MorphiumId/byte[]) mixed in - those are reassembled by
     * {@code restoreDumpValue} and would break first if a value were ever split across a refill.
     */
    private InMemoryDriver sourceDriver(int docCount) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < docCount; i++) {
            Map<String, Object> doc = Doc.of("_id", "doc-" + i,
                                             "counter", i,
                                             "text", "Ümläute, \"Quotes\" und ein Zeilenumbruch\n in Dokument " + i,
                                             "when", date,
                                             "uuid", uuid,
                                             "mid", id);
            doc.put("bin", binary);
            docs.add(doc);
        }

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        return drv;
    }

    private void assertRestoredFaithfully(InMemoryDriver target, int docCount, String what) {
        List<Map<String, Object>> restored = target.getDatabase(db).get(coll);
        assertEquals(docCount, restored.size(), what + ": every document must come back");

        for (int i = 0; i < docCount; i++) {
            Map<String, Object> doc = restored.get(i);
            assertEquals("doc-" + i, doc.get("_id"), what + ": id of document " + i);
            assertEquals("Ümläute, \"Quotes\" und ein Zeilenumbruch\n in Dokument " + i, doc.get("text"),
                    what + ": text of document " + i + " - a value split across a read must not change");
            assertEquals(date, doc.get("when"), what + ": Date of document " + i);
            assertEquals(uuid, doc.get("uuid"), what + ": UUID of document " + i);
            assertEquals(id, doc.get("mid"), what + ": MorphiumId of document " + i);
            assertArrayEquals(binary, (byte[]) doc.get("bin"), what + ": binary of document " + i);
        }
    }

    @Test
    public void aDumpReadInSmallChunksRestoresIdentically(@TempDir Path tmp) throws Exception {
        int docCount = 200;
        InMemoryDriver source = sourceDriver(docCount);
        File f = new File(tmp.toFile(), "chunked.morphium.gz");
        source.dumpToFile(db, f);
        assertTrue(f.length() > 0, "sanity: the dump must have been written");

        // 10_000 is the realistic read size, 64 forces a refill every few values, 1 is the
        // degenerate case where no read ever returns more than a single character.
        for (int chunk : new int[] {10_000, 64, 1}) {
            InMemoryDriver target = new InMemoryDriver();
            target.dumpRestoreChunkChars = chunk;
            target.restoreFromFile(f);
            assertRestoredFaithfully(target, docCount, "chunk=" + chunk);
        }
    }

    // A test that asserted "the restore never calls readAllBytes()" used to live here, using an
    // InputStream that threw from readAllBytes()/readNBytes(). It proved nothing: GZIPInputStream
    // does not override readAllBytes(), so the default implementation loops on read(byte[],off,len)
    // against ITSELF and reaches the wrapped stream only through fill() - the overrides were never
    // called, and the test passed against the pre-fix code that still slurped. Verified by running
    // it against 5a3125911. The chunked round-trip above is the real evidence: it fails the moment
    // the parse stops being incremental.

    /**
     * The one behaviour the streaming restore cannot keep: a legacy non-UTF-8 dump (#306) is only
     * recognisable once a failed decode has consumed part of the stream, and a plain
     * {@code InputStream} cannot be rewound for the ISO-8859-1 retry. The file-based entry points
     * reopen and still handle it (see {@code InMemDumpRoundtripTest.legacyLatin1DumpIsStillReadable});
     * the stream-based one must say so instead of failing obscurely.
     */
    @Test
    public void aLegacyDumpOnAPlainStreamFailsWithAnActionableMessage(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"legacy_stream\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"doc1\", \"text\" : \"Müller äöüß\" } ] } }";
        File f = new File(tmp.toFile(), "legacy-stream.morphium.gz");

        try (FileOutputStream fos = new FileOutputStream(f);
            GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write(json.getBytes(StandardCharsets.ISO_8859_1));
        }

        InMemoryDriver target = new InMemoryDriver();
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> target.restore(new FileInputStream(f)));
        assertTrue(ex.getMessage().contains("restoreFromFile"),
                "the message must point at the entry point that can still read it: " + ex.getMessage());

        // ... and that entry point does read it.
        InMemoryDriver viaFile = new InMemoryDriver();
        viaFile.restoreFromFile(f);
        assertEquals("Müller äöüß",
                viaFile.getDatabase("legacy_stream").get("coll").get(0).get("text"));
    }

    /**
     * The parse-error message used to quote the text around the failure out of the full dump
     * String. With the dump never in memory as a whole, the last characters the parser consumed
     * have to carry that job - losing the position and the context entirely would undo #306's
     * hardest-won diagnostic.
     */
    @Test
    public void aBrokenDumpStillReportsPositionAndSurroundingText(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"broken\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"doc1\", \"marker_text_here\" : ]] } ] } }";
        File f = new File(tmp.toFile(), "broken.morphium.gz");

        try (FileOutputStream fos = new FileOutputStream(f);
            GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write(json.getBytes(StandardCharsets.UTF_8));
        }

        InMemoryDriver target = new InMemoryDriver();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> target.restoreFromFile(f));
        assertTrue(ex.getMessage().contains("invalid JSON at position"),
                "the parse position must stay in the message: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("marker_text_here"),
                "the text leading up to the error must stay in the message: " + ex.getMessage());
    }

}
