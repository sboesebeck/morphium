package de.caluga.test.mongo.suite.bson;

import com.mongodb.BasicDBObject;
import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.test.mongo.suite.MongoTest;
import org.junit.Test;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.10.15
 * Time: 23:16
 * <p>
 * TODO: Add documentation here
 */
public class ConnectTest extends MongoTest {
    private static String[] chars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",};

    private Logger log = new Logger(ConnectTest.class);

    @Test
    public void testConnection() throws Exception {

        Socket s = new Socket("localhost", 27017);

        OutputStream out = s.getOutputStream();
        InputStream in = s.getInputStream();

        //Msg...
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writeInt(1, buffer); //request-id
        writeInt(0, buffer); //answer
        writeInt(2004, buffer); //opcode OP_QUERY
        writeInt(0, buffer); //flags
        writeString("tst.test_coll", buffer);
        writeInt(0, buffer); //number to skip
        writeInt(10, buffer); //return

        Map<String, Object> query = new HashMap<>();
        HashMap<String, Object> q = new HashMap<String, Object>();
//        q.put("_id", new MongoId());
        query.put("$query", q);
//        query.put("_id", new MongoId());
//        BsonEncoder enc = new BsonEncoder();
        byte[] bytes = BsonEncoder.encodeDocument(query);
        buffer.write(bytes);

        writeInt(buffer.size() + 4, out);
        out.write(buffer.toByteArray());
        out.flush();

        log.info("query sent...");

        byte[] inBuffer = new byte[1024];

        int numRead = -1;

        while (numRead == -1) {
            numRead = in.read(inBuffer);
        }
        log.info("read: " + numRead + " bytes");

        log.info(getHex(inBuffer));

        int sz = readInt(inBuffer, 0);
        int reqId = readInt(inBuffer, 4);
        int inReplyTo = readInt(inBuffer, 8);
        int opCode = readInt(inBuffer, 12);
        int flags = readInt(inBuffer, 16);
        long cursorId = readLong(inBuffer, 20);
        int startFrom = readInt(inBuffer, 28);
        int numReturned = readInt(inBuffer, 32);
        log.info("Size  :         " + sz);
        log.info("reqId :         " + getHex(reqId));
        log.info("inRepl:         " + getHex(inReplyTo));
        log.info("flags:          " + getHex(flags));
        log.info("cursor: " + getHex(cursorId));
        log.info("startFrom:      " + getHex(startFrom));
        log.info("returned docs : " + getHex(numReturned));

        BsonDecoder dec = new BsonDecoder();
        int idx = 36;
        for (int i = 0; i < numReturned; i++) {
            Map<String, Object> obj = new HashMap<>();
            int l = dec.decodeDocumentIn(obj, inBuffer, idx);
            idx += l;

            log.info(new BasicDBObject(obj).toString());
        }


    }


    public abstract class MsgHeader {
        int reqId;
        int inReplyTo;


        public abstract byte[] getPayload() throws IOException;

        public abstract MsgHeader parse(byte[] bytes, int offset) throws UnsupportedEncodingException;


        public byte[] bytes() throws IOException {
            byte[] payload = getPayload();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ConnectTest.this.writeInt(payload.length + 8, out);
            ConnectTest.this.writeInt(reqId, out);
            ConnectTest.this.writeInt(inReplyTo, out);
            out.write(payload);
            return out.toByteArray();
        }

        public void parseHeader(byte[] bytes) {
            int size = readInt(bytes, 0);
            reqId = readInt(bytes, 4);
            inReplyTo = readInt(bytes, 8);
        }
    }

    public class OPQuery extends MsgHeader {
        int opCode = 2004;
        int flags;
        String db;
        String coll;
        int skip;
        int limit;
        Map<String, Object> doc;

        @Override
        public byte[] getPayload() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeInt(opCode, out);
            writeInt(flags, out);
            writeString(db + "." + coll, out);
            writeInt(skip, out);
            writeInt(limit, out);
            out.write(BsonEncoder.encodeDocument(doc));
            return out.toByteArray();
        }

        @Override
        public MsgHeader parse(byte[] bytes, int offset) {
            //does not make any sense...
            return null;
        }
    }

    public class OPReply extends MsgHeader {
        int opcode = 1;
        int flags;
        long cursorId;
        int startFrom;
        int numReturned;
        List<Map<String, Object>> documents;

        @Override
        public byte[] getPayload() {
            return new byte[0];
        }

        @Override
        public MsgHeader parse(byte[] bytes, int offset) throws UnsupportedEncodingException {
            super.parseHeader(bytes);
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
            return this;
        }
    }


    public void writeInt(int value, OutputStream to) throws IOException {
        to.write(((byte) ((value) & 0xff)));
        to.write(((byte) ((value >> 8) & 0xff)));
        to.write(((byte) ((value >> 16) & 0xff)));
        to.write(((byte) ((value >> 24) & 0xff)));
    }

    public int readInt(byte[] bytes, int idx) {
        return bytes[idx] | (bytes[idx + 1] << 8) | (bytes[idx + 2] << 16) | (bytes[idx + 3] << 24);
    }

    public long readLong(byte[] bytes, int idx) {
        return ((long) ((bytes[idx] & 0xFF))) |
                ((long) ((bytes[idx + 1] & 0xFF)) << 8) |
                ((long) (bytes[idx + 2] & 0xFF) << 16) |
                ((long) (bytes[idx + 3] & 0xFF) << 24) |
                ((long) (bytes[idx + 4] & 0xFF) << 32) |
                ((long) (bytes[idx + 5] & 0xFF) << 40) |
                ((long) (bytes[idx + 6] & 0xFF) << 48) |
                ((long) (bytes[idx + 7] & 0xFF) << 56);

    }

    public void writeString(String n, OutputStream to) throws IOException {
        to.write(n.getBytes("UTF-8"));
        to.write((byte) 0);
    }


    public String getHex(byte[] b) {
        StringBuilder sb = new StringBuilder();

        int mainIdx = 0;


        while (mainIdx < b.length) {
            sb.append(getHex((byte) (mainIdx >> 24 & 0xff)));
            sb.append(getHex((byte) (mainIdx >> 16 & 0xff)));
            sb.append(getHex((byte) (mainIdx >> 8 & 0xff)));
            sb.append(getHex((byte) (mainIdx & 0xff)));

            sb.append(":  ");
            for (int i = mainIdx; i < mainIdx + 16 && i < b.length; i++) {
                byte by = b[i];
                sb.append(getHex(by));
                sb.append(" ");
            }

            try {
                int l = 16;
                if (mainIdx + 16 > b.length) {
                    l = b.length - mainIdx;
                }

                byte sr[] = new byte[l];
                int n = 0;
                for (int j = mainIdx; j < mainIdx + l; j++) {
                    if (b[j] < 128 && b[j] > 63) {
                        sr[n] = b[j];
                    } else if (b[j] == 0) {
                        sr[n] = '-';
                    } else {
                        sr[n] = '.';
                    }
                    n++;
                }
                String str = new String(sr, 0, l, "UTF-8");
                sb.append("    ");
                sb.append(str);
            } catch (UnsupportedEncodingException e) {

            }
            sb.append("\n");
            mainIdx += 16;
        }
        return sb.toString();

    }


    private String getHex(long i) {
        return (getHex((byte) (i >> 56 & 0xff)) + getHex((byte) (i >> 48 & 0xff)) + getHex((byte) (i >> 40 & 0xff)) + getHex((byte) (i >> 32 & 0xff)) + getHex((byte) (i >> 24 & 0xff)) + getHex((byte) (i >> 16 & 0xff)) + getHex((byte) (i >> 8 & 0xff)) + getHex((byte) (i & 0xff)));
    }

    private String getHex(int i) {
        return (getHex((byte) (i >> 24 & 0xff)) + getHex((byte) (i >> 16 & 0xff)) + getHex((byte) (i >> 8 & 0xff)) + getHex((byte) (i & 0xff)));
    }

    private String getHex(byte by) {
        String ret = "";
        int idx = (by >>> 4) & 0x0f;
        ret += chars[idx];
        idx = by & 0x0f;
        ret += chars[idx];
        return ret;
    }
}
