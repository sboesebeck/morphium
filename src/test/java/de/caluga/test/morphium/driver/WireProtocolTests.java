package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WireProtocolTests {
    private Logger log = LoggerFactory.getLogger(WireProtocolTests.class);
    @Test
    public void testOpMsg() throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(123);
        msg.setResponseTo(42);
        msg.setFlags(OpMsg.EXHAUST_ALLOWED);
        msg.setFirstDoc(Doc.of("hello", 1));
        byte[] data = msg.bytes();
        WireProtocolMessage wp = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertNotNull(wp);
        ;
        assert(wp instanceof OpMsg);
    }

    @Test
    public void compressionTests() throws Exception {
        String payload = "foo bar foo bar and so on - need some length to make it shorter";
        payload = payload + payload + payload;
        byte[] data = payload.getBytes();
        byte[] compressed = Snappy.compress(data);
        log.info("Snappy: original data length: " + data.length + " compressed: " + compressed.length);
        byte[] uncompressed = Snappy.uncompress(compressed);
        assertEquals(data.length, uncompressed.length);
        assertEquals(payload, new String(uncompressed));
        //zlib
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DeflaterOutputStream out = new DeflaterOutputStream(bout);
        out.write(data);
        out.close();
        out.flush();
        compressed = bout.toByteArray();
        log.info("Zlib: original data length: " + data.length + " compressed: " + compressed.length);
        InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(compressed));
        byte[] b = new byte[100];
        int r = 0;
        var o = new ByteArrayOutputStream();

        while ((r = in.read(b, 0, 100)) != -1) {
            o.write(b, 0, r);
        }

        o.close();
        uncompressed = o.toByteArray();
        assertEquals(data.length, uncompressed.length);
        assertEquals(payload, new String(uncompressed));
    }

    @Test
    public void testOpCompressedZlib() throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(123);
        msg.setResponseTo(42);
        msg.setFlags(OpMsg.EXHAUST_ALLOWED);
        msg.setFirstDoc(Doc.of("hello", 1));
        OpCompressed cmp = new OpCompressed();
        cmp.setUncompressedSize(msg.getSize());
        cmp.setMessageId(msg.getMessageId());
        cmp.setCompressedMessage(msg.bytes());
        cmp.setCompressorId(OpCompressed.COMPRESSOR_ZLIB);
        byte[] data = cmp.bytes();
        OpCompressed cmp2 = (OpCompressed)WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        var wp = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(cmp2.getCompressedMessage()));
        assertTrue(wp instanceof OpMsg);
    }

    @Test
    public void testOpCompressedSnappy() throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(123);
        msg.setResponseTo(42);
        msg.setFlags(OpMsg.EXHAUST_ALLOWED);
        msg.setFirstDoc(Doc.of("hello", 1));
        OpCompressed cmp = new OpCompressed();
        cmp.setUncompressedSize(msg.getSize());
        cmp.setMessageId(msg.getMessageId());
        cmp.setCompressedMessage(msg.bytes());
        cmp.setCompressorId(OpCompressed.COMPRESSOR_SNAPPY);
        byte[] data = cmp.bytes();
        OpCompressed cmp2 = (OpCompressed)WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        log.info("Len: " + cmp2.getCompressedMessage().length);
        var wp = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(cmp2.getCompressedMessage()));
        assertTrue(wp instanceof OpMsg);
    }
}
