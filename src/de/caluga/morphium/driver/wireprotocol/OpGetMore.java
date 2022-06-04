package de.caluga.morphium.driver.wireprotocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OpGetMore extends WireProtocolMessage {
    private int numberToReturn;
    private long cursorId;
    private String fullCollectionName;


    public int getNumberToReturn() {
        return numberToReturn;
    }

    public void setNumberToReturn(int numberToReturn) {
        this.numberToReturn = numberToReturn;
    }

    public long getCursorId() {
        return cursorId;
    }

    public void setCursorId(long cursorId) {
        this.cursorId = cursorId;
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }

    public void setFullCollectionName(String fullCollectionName) {
        this.fullCollectionName = fullCollectionName;
    }

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        //ZERO
        int zero = readInt(bytes, offset);
        assert (zero == 0);
        fullCollectionName = readString(bytes, offset + 4);
        offset = offset + 4 + strLen(bytes, offset + 4);
        numberToReturn = readInt(bytes, offset);
        cursorId = readLong(bytes, offset + 4);
    }

    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(0, out);
        writeString(fullCollectionName, out);
        writeInt(numberToReturn, out);
        writeLong(cursorId, out);
        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_GET_MORE.opCode;
    }
}
