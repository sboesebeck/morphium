package de.caluga.morphium.driver.wireprotocol;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class OpUpdate extends WireProtocolMessage {
    public static final int UPSERT_FLAG = 1;
    public static final int MULTI_FLAG = 2;

    private String fullCollectionName;
    private int flags;
    private Doc selector;
    private Doc update;

    public String getFullCollectionName() {
        return fullCollectionName;
    }

    public void setFullCollectionName(String fullCollectionName) {
        this.fullCollectionName = fullCollectionName;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public Doc getSelector() {
        return selector;
    }

    public void setSelector(Doc selector) {
        this.selector = selector;
    }

    public Doc getUpdate() {
        return update;
    }

    public void setUpdate(Doc update) {
        this.update = update;
    }

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        int zero = readInt(bytes, offset);
        assert (zero == 0);
        fullCollectionName = readString(bytes, offset + 4);
        int idx = offset + 4 + strLen(bytes, offset + 4);
        flags = readInt(bytes, idx);
        idx += 4;
        selector = new Doc();
        idx = idx + BsonDecoder.decodeDocumentIn(selector, bytes, idx);
        update = new Doc();
        idx = idx + BsonDecoder.decodeDocumentIn(update, bytes, idx);
    }

    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        writeInt(0, out);
        writeString(fullCollectionName, out);
        writeInt(flags, out);
        out.write(BsonEncoder.encodeDocument(selector));
        out.write(BsonEncoder.encodeDocument(update));
        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_UPDATE.opCode;
    }
}
