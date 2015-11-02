package de.caluga.test.mongo.suite.bson;

import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.bson.MongoId;
import de.caluga.test.mongo.suite.MongoTest;
import org.junit.Test;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
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
        q.put("_id", new MongoId());
        query.put("$query", q);
//        query.put("_id", new MongoId());
        BsonEncoder enc = new BsonEncoder();
        byte[] bytes = enc.encodeDocument(query);
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


    }


    public void writeInt(int value, OutputStream to) throws IOException {
        to.write(((byte) ((value) & 0xff)));
        to.write(((byte) ((value >> 8) & 0xff)));
        to.write(((byte) ((value >> 16) & 0xff)));
        to.write(((byte) ((value >> 24) & 0xff)));


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

    private String getHex(byte by) {
        String ret = "";
        int idx = (by >>> 4) & 0x0f;
        ret += chars[idx];
        idx = by & 0x0f;
        ret += chars[idx];
        return ret;
    }
}
