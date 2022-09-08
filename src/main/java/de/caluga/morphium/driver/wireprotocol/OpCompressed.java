package de.caluga.morphium.driver.wireprotocol;

import java.io.IOException;

public class OpCompressed extends WireProtocolMessage {
    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {

    }

    @Override
    public byte[] getPayload() throws IOException {
        return new byte[0];
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_COMPRESSED.opCode;
    }
}
