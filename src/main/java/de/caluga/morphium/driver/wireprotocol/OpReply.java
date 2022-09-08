package de.caluga.morphium.driver.wireprotocol;/**
 * Created by stephan on 04.11.15.
 */


import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OPReply implemtation for mongodb wire protocol
 **/

public class OpReply extends WireProtocolMessage {
    public static final int CURSOR_NOT_FOUND_FLAG = 1;
    public static final int QUERY_FAILURE_FLAG = 2;
    public static final int SHARD_CONFIG_STATE_FLAG = 4;
    public static final int AWAIT_CAPABLE_FLAG = 8;

    public final long timestamp;
    private int size = 0;
    private int opcode = 1;
    private int flags;
    private long cursorId;
    private int startFrom;
    private int numReturned;
    private List<Map<String, Object>> documents;

    public OpReply() {
        timestamp = System.currentTimeMillis();
    }


    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(flags, out);
        writeLong(cursorId, out);
        writeInt(startFrom, out);
        writeInt(numReturned, out);
        for (Map<String, Object> doc : documents) {
            out.write(BsonEncoder.encodeDocument(doc));
        }
        return out.toByteArray();
    }

    @SuppressWarnings("unused")
    public int getSize() {
        return size;
    }

    @SuppressWarnings("unused")
    public void setSize(int size) {
        this.size = size;
    }

    @SuppressWarnings("unused")
    public int getOpcode() {
        return opcode;
    }

    @SuppressWarnings("unused")
    public void setOpcode(int opcode) {
        this.opcode = opcode;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public long getCursorId() {
        return cursorId;
    }

    @SuppressWarnings("unused")
    public void setCursorId(long cursorId) {
        this.cursorId = cursorId;
    }

    public int getStartFrom() {
        return startFrom;
    }

    @SuppressWarnings("unused")
    public void setStartFrom(int startFrom) {
        this.startFrom = startFrom;
    }

    @SuppressWarnings("unused")
    public int getNumReturned() {
        return numReturned;
    }

    @SuppressWarnings("unused")
    public void setNumReturned(int numReturned) {
        this.numReturned = numReturned;
    }

    public List<Map<String, Object>> getDocuments() {
        return documents;
    }

    @SuppressWarnings("unused")
    public void setDocuments(List<Map<String, Object>> documents) {
        this.documents = documents;
    }

    public void addDocument(Map<String, Object> doc) {
        if (documents == null) documents = new ArrayList<>();
        documents.add(doc);
    }

    public void parsePayload(byte[] bytes, int offset) throws IOException {
        flags = readInt(bytes, offset);
        offset += 4;
        cursorId = readLong(bytes, offset);
        offset += 8;
        startFrom = readInt(bytes, offset);
        offset += 4;
        numReturned = readInt(bytes, offset);
        offset += 4;


        documents = new ArrayList<>();
        for (int i = 0; i < numReturned; i++) {
            Doc m = new Doc();
            int l = BsonDecoder.decodeDocumentIn(m, bytes, offset);
            offset += l;
            documents.add(m);
        }

    }

    @SuppressWarnings("unused")
    public int getOpCode() {
        return opcode;
    }
}
