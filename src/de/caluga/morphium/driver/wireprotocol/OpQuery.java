package de.caluga.morphium.driver.wireprotocol;/**
 * Created by stephan on 04.11.15.
 */

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Query call implementation for MongoDB wire protocol
 **/
public class OpQuery extends WireProtocolMessage {
    public static final int TAILABLE_CURSOR_FLAG = 2;
    public static final int SLAVE_OK_FLAG = 4;
    public static final int OPLOG_REPLAY_FLAG = 8;
    public static final int NO_CURSOR_TIMEOUT_FLAG = 16;
    public static final int AWAIT_DATA_FLAG = 32;
    public static final int EXHAUST_FLAG = 64;
    public static final int PARTIAL_FLAG = 128;
    private static final int OP_CODE = 2004;
    private String db;
    private String coll;
    private int skip = 0;
    private int limit = 1;
    private Doc doc;

//    private int reqId;
//    private int inReplyTo;
private int flags;
    private Doc returnFieldSelector;


    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @SuppressWarnings("unused")
    public String getColl() {
        return coll;
    }

    public void setColl(String coll) {
        this.coll = coll;
    }

    @SuppressWarnings("unused")
    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    @SuppressWarnings("unused")
    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @SuppressWarnings("unused")
    public Doc getDoc() {
        return doc;
    }

    public void setDoc(Doc doc) {
        this.doc = doc;
    }
//
//    public int getReqId() {
//        return reqId;
//    }
//
//    public void setReqId(int reqId) {
//        this.reqId = reqId;
//    }
//
//    @SuppressWarnings("unused")
//    public int getInReplyTo() {
//        return inReplyTo;
//    }
//
//    public void setInReplyTo(int inReplyTo) {
//        this.inReplyTo = inReplyTo;
//    }

    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(getOpCode(), out);
        writeInt(flags, out);
        writeString(db + "." + coll, out);
        writeInt(skip, out);
        writeInt(limit, out);
        out.write(BsonEncoder.encodeDocument(doc));
        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OP_CODE;
    }


    @Override
    public void parsePayload(byte[] bytes, int offset) {
        doc = new Doc();
        flags = readInt(bytes, offset);
        offset += 4;
        coll = readString(bytes, offset);
        offset += coll.getBytes(StandardCharsets.UTF_8).length + 1;
        skip = readInt(bytes, offset);
        offset += 4;
        limit = readInt(bytes, offset);
        offset += 4;
        try {
            int sz = BsonDecoder.decodeDocumentIn(doc, bytes, offset);
            if (sz + offset < bytes.length) {
                //another doc?
                returnFieldSelector = new Doc();
                BsonDecoder.decodeDocumentIn(returnFieldSelector, bytes, offset + sz);
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public String toString() {
        return "OpQuery{" +
                "db='" + db + '\'' +
                ", coll='" + coll + '\'' +
                ", skip=" + skip +
                ", limit=" + limit +
                ", doc=" + Utils.toJsonString(doc) +
                ", flags=" + flags +
                ", returnFieldsSelector=" + Utils.toJsonString(returnFieldSelector) +
                '}';
    }
}
