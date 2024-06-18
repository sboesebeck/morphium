package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

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
        var wp = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(cmp2.getCompressedMessage()));
        assertTrue(wp instanceof OpMsg);
    }
}
