package de.caluga.morphium.driver.wireprotocol;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class OpDelete extends WireProtocolMessage {
    private int flags;
    private String fullCollectionName;
    private Doc selector;

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }

    public void setFullCollectionName(String fullCollectionName) {
        this.fullCollectionName = fullCollectionName;
    }

    public Map<String, Object> getSelector() {
        return selector;
    }

    public void setSelector(Doc selector) {
        this.selector = selector;
    }

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        //zero at offset
        int zero = readInt(bytes, offset);
        assert (zero == 0);
        fullCollectionName = readString(bytes, offset + 4);
        int strLen = strLen(bytes, offset + 4);
        flags = readInt(bytes, offset + 4 + strLen);
        selector = new Doc();
        BsonDecoder.decodeDocumentIn(selector, bytes, offset + 8 + strLen);
    }

    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(0, out);
        writeString(fullCollectionName, out);
        writeInt(flags, out);
        out.write(BsonEncoder.encodeDocument(selector));
        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_DELETE.opCode;
    }
}
