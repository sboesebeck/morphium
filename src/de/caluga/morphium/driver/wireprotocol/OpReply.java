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
 * OPReply implemtation for mongodb wire protocol
 **/

public class OpReply extends WireProtocolMessage {
    public final long timestamp;
    private int reqId;
    private int inReplyTo;
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

    public void parse(byte[] bytes) throws UnsupportedEncodingException {
        parse(bytes, 0);
    }

    public int getReqId() {
        return reqId;
    }

    @SuppressWarnings("unused")
    public void setReqId(int reqId) {
        this.reqId = reqId;
    }

    public int getInReplyTo() {
        return inReplyTo;
    }

    @SuppressWarnings("unused")
    public void setInReplyTo(int inReplyTo) {
        this.inReplyTo = inReplyTo;
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

    private void parse(byte[] bytes, int offset) throws UnsupportedEncodingException {
        size = readInt(bytes, offset);
        offset += 4;
        reqId = readInt(bytes, offset);
        offset += 4;
        inReplyTo = readInt(bytes, offset);
        offset += 4;
        opcode = readInt(bytes, offset);
        offset += 4;
        if (opcode != 1) {
            throw new IllegalArgumentException("Unknown Opcode " + opcode);
        }
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

    @SuppressWarnings("unused")
    public int getOpCode() {
        return opcode;
    }
}
