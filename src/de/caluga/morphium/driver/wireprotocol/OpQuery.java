package de.caluga.morphium.driver.wireprotocol;/**
 * Created by stephan on 04.11.15.
 */

import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class OpQuery extends WireProtocolMessage {
    int opCode = 2004;

    String db;
    String coll;
    int skip = 0;
    int limit = 1;
    Map<String, Object> doc;

    int reqId;
    int inReplyTo;


    public int getOpCode() {
        return opCode;
    }

    public void setOpCode(int opCode) {
        this.opCode = opCode;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getColl() {
        return coll;
    }

    public void setColl(String coll) {
        this.coll = coll;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public Map<String, Object> getDoc() {
        return doc;
    }

    public void setDoc(Map<String, Object> doc) {
        this.doc = doc;
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

    public byte[] bytes() throws IOException {
        byte[] payload = getPayload();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(payload.length + 12, out);
        writeInt(reqId, out);
        writeInt(inReplyTo, out);
        out.write(payload);
        return out.toByteArray();
    }

    private byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(opCode, out);
        writeInt(flags, out);
        writeString(db + "." + coll, out);
        writeInt(skip, out);
        writeInt(limit, out);
        out.write(BsonEncoder.encodeDocument(doc));
        return out.toByteArray();
    }
}
