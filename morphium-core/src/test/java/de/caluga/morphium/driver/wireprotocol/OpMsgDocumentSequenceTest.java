package de.caluga.morphium.driver.wireprotocol;

import de.caluga.morphium.driver.Doc;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OP_MSG kind-1 (document sequence) sections: mongorestore/mongoimport ship bulk inserts this
 * way. morphium's own clients only ever send kind-0, so this path is exercised exclusively by
 * foreign drivers talking to PoppyDB.
 */
@Tag("driver")
public class OpMsgDocumentSequenceTest {

    @Test
    public void kind1SectionsParseAndStopAtMessageEnd() throws Exception {
        OpMsg out = new OpMsg();
        out.setFirstDoc(Doc.of("insert", "kunden", "$db", "test"));
        out.addDoc("documents", Doc.of("_id", 1, "name", "a"));
        out.addDoc("documents", Doc.of("_id", 2, "name", "b"));
        byte[] payload = out.getPayload();

        // Simulate PoppyDB's zero-copy decode path: the backing array holds junk before the
        // payload (offset) and further pipelined bytes after it - the parse must honor the
        // declared message size instead of running to the end of the array. Before the fix it
        // read into the trailing bytes ("unknown data type: 100"/garbage section ids).
        byte[] buffer = new byte[8 + payload.length + 32];
        System.arraycopy(payload, 0, buffer, 8, payload.length);
        Arrays.fill(buffer, 8 + payload.length, buffer.length, (byte) 0x64);

        OpMsg in = new OpMsg();
        in.setSize(payload.length + 16);   // wire size includes the 16-byte header
        in.parsePayload(buffer, 8);

        assertEquals("kunden", in.getFirstDoc().get("insert"));
        assertNotNull(in.getDocuments());
        assertEquals(2, in.getDocuments().get("documents").size());
        assertEquals(2, ((Number) in.getDocuments().get("documents").get(1).get("_id")).intValue());
    }

    @Test
    public void parseWithoutSizeFallsBackToArrayLength() throws Exception {
        // Exact-array convention (OpCompressed unwrap paths call parsePayload without setSize)
        OpMsg out = new OpMsg();
        out.setFirstDoc(Doc.of("ping", 1, "$db", "admin"));
        byte[] payload = out.getPayload();

        OpMsg in = new OpMsg();
        in.parsePayload(payload, 0);

        assertEquals(1, ((Number) in.getFirstDoc().get("ping")).intValue());
        assertNull(in.getDocuments());
    }
}
