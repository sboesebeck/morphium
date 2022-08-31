package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

public class WireProtocolTests {

    @Test
    public void testOpMsg() throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(123);
        msg.setResponseTo(42);
        msg.setFlags(OpMsg.EXHAUST_ALLOWED);
        msg.setFirstDoc(Doc.of("hello", 1));

        byte[] data = msg.bytes();

        WireProtocolMessage wp = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assert (wp != null);
        assert (wp instanceof OpMsg);

    }
}
