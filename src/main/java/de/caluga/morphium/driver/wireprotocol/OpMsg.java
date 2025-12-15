package de.caluga.morphium.driver.wireprotocol;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.CRC32C;

/**
 * see https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst
 * <p>
 * OP_MSG {
 * MsgHeader header;          // standard message header
 * flagBits;           // message flags
 * Sections[] sections;       // data sections
 * checksum; // optional CRC-32C checksum
 * }
 * struct MsgHeader {
 * int32   messageLength; // total message size, including this
 * int32   requestID;     // identifier for this message
 * int32   responseTo;    // requestID from the original request
 * //   (used in responses from db)
 * int32   opCode;        // request type - see table below for details
 * }
 * <p>
 * <p>
 * section type 0 (BASIC):
 * byte 0;
 * BSON-Document
 * <p>
 * section type 1 (optimized):
 * byte 1;
 * int32 size
 * CString sequence id;  //reference to insert, id is "documents"/ to update id "updates", to delete id is "deletes"
 * BSON-Documents
 * <p>
 * BSON-Document e.g: {insert: "test_coll", $db: "db", documents: [{_id: 1}]}
 */
public class OpMsg extends WireProtocolMessage {
    public static final int OP_CODE = 2013;


    public static final int CHECKSUM_PRESENT = 1;
    public static final int MORE_TO_COME = 2;
    public static final int EXHAUST_ALLOWED = 65536;


    private Map<String, Object> firstDoc;
    private Map<String, List<Map<String, Object>>> documents;

    private int flags;

    public void addDoc(String seqId, Map<String, Object> o) {
        if (documents == null) documents = new LinkedHashMap<>();
        documents.putIfAbsent(seqId, new ArrayList<>());
        documents.get(seqId).add(o);
    }

    public Map<String, Object> getFirstDoc() {
        return firstDoc;
    }

    public OpMsg setFirstDoc(Map<String, Object> o) {
        firstDoc = o;
        return this;
    }


    public int getFlags() {
        return flags;
    }

    public OpMsg setFlags(int flags) {
        this.flags = flags;
        return this;
    }

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        flags = readInt(bytes, offset);
        int idx = offset + 4;
        int len = bytes.length;
        if ((getFlags() & CHECKSUM_PRESENT) != 0) {
            len = bytes.length - 4;
        }

        while (idx < len) {
            //reading in sections
            byte section = bytes[idx];
            idx++;
            if (section == 0) {
                Doc result = new Doc();
                int l = BsonDecoder.decodeDocumentIn(result, bytes, idx);
                firstDoc = result;
                idx += l;
            } else if (section == 1) {
                //size
                //sequence ID
                //Documents
                int size = readInt(bytes, idx);
                String seqId = readString(bytes, idx + 4);
                int strLen = strLen(bytes, idx + 4);
                int i = 0;
                while (4 + strLen + i < size) {
                    Doc doc = new Doc();
                    i += BsonDecoder.decodeDocumentIn(doc, bytes, idx + 4 + strLen + i);
                    addDoc(seqId, doc);
                }
                idx += 4 + strLen + i;
            } else {
                throw new RuntimeException("wrong section ID " + section);
            }
        }
        if ((getFlags() & CHECKSUM_PRESENT) != 0) {
            int crc = readInt(bytes, idx);
            CRC32C c = new CRC32C();
            c.update(bytes, 0, bytes.length - 4);
            assert (crc == ((int) c.getValue()));
        }
    }

    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(flags, out);
        out.write((byte) 0); //section basic
        byte[] d = BsonEncoder.encodeDocument(firstDoc);
        out.write(d);
        if (documents != null) {
            for (String seqId : documents.keySet()) {
                ByteArrayOutputStream sectionOut = new ByteArrayOutputStream();
                writeString(seqId, sectionOut);
                for (var doc : documents.get(seqId)) {
                    sectionOut.write(BsonEncoder.encodeDocument(doc));
                }
                byte[] section = sectionOut.toByteArray();
                writeInt(section.length, out);
            }
        }
        byte[] ret = out.toByteArray();
        if ((getFlags() & CHECKSUM_PRESENT) != 0) {
            //CRC32 checksum
            CRC32C crc = new CRC32C();
            crc.update(ret);
            writeInt((int) crc.getValue(), out);
            ret = out.toByteArray();
        }
        return ret;
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_MSG.opCode;
    }

    public boolean hasCursor() {
        return firstDoc != null && firstDoc.containsKey("cursor");
    }

}
