package de.caluga.morphium.driver.wireprotocol;/**
 * Created by stephan on 04.11.15.
 */

import de.caluga.morphium.driver.bson.BsonDecoder;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/

public class OpReply extends WireProtocolMessage {
    int reqId;
    int inReplyTo;

    int size = 0;
    int opcode = 1;
    int flags;
    long cursorId;
    int startFrom;
    int numReturned;
    byte[] readBytes;
    public long timestamp;
    List<Map<String, Object>> documents;

    public OpReply() {
        timestamp = System.currentTimeMillis();
    }

    public void parse(byte[] bytes) throws UnsupportedEncodingException {
        readBytes = bytes;
        parse(bytes, 0);
    }

    public int getReqId() {
        return reqId;
    }

    public void setReqId(int reqId) {
        this.reqId = reqId;
    }

    public int getInReplyTo() {
        return inReplyTo;
    }

    public void setInReplyTo(int inReplyTo) {
        this.inReplyTo = inReplyTo;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getOpcode() {
        return opcode;
    }

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

    public void setCursorId(long cursorId) {
        this.cursorId = cursorId;
    }

    public int getStartFrom() {
        return startFrom;
    }

    public void setStartFrom(int startFrom) {
        this.startFrom = startFrom;
    }

    public int getNumReturned() {
        return numReturned;
    }

    public void setNumReturned(int numReturned) {
        this.numReturned = numReturned;
    }

    public List<Map<String, Object>> getDocuments() {
        return documents;
    }

    public void setDocuments(List<Map<String, Object>> documents) {
        this.documents = documents;
    }

    private void parse(byte[] bytes, int offset) throws UnsupportedEncodingException {
        size = readInt(bytes, offset);
        offset += 4;
        reqId = readInt(bytes, offset);
        offset += 4;
        inReplyTo = readInt(bytes, offset);
        offset += 4;
        opcode = readInt(bytes, offset);
        offset += 4;
        if (opcode != 1) throw new IllegalArgumentException("Unknown Opcode " + opcode);
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
            Map<String, Object> m = new HashMap<>();
            BsonDecoder dec = new BsonDecoder();
            int l = dec.decodeDocumentIn(m, bytes, offset);
            offset += l;
            documents.add(m);
        }

    }

    public int getOpCode() {
        return opcode;
    }
}
