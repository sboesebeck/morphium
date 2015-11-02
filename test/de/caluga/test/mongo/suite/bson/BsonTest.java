package de.caluga.test.mongo.suite.bson;/**
 * Created by stephan on 28.10.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.bson.MongoId;
import de.caluga.test.mongo.suite.MongoTest;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * TODO: Add Documentation here
 **/
public class BsonTest extends MongoTest {

    private static String[] chars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",};

    private static Logger log = new Logger(BsonTest.class);


    @Test
    public void encodeDecodeTest() throws Exception {

        Map<String, Object> doc = new HashMap<>();

        doc.put("_id", new MongoId());
        doc.put("counter", 123);
        doc.put("value", "a value");

        Map<String, Object> subDoc = new HashMap<>();
        subDoc.put("some", 1223.2);
        subDoc.put("bool", true);
        subDoc.put("created", new Date());

        doc.put("sub", subDoc);


        byte[] bytes = BsonEncoder.encodeDocument(doc);

        log.error(getHex(bytes));
//        System.err.println(getHex(bytes));

        BsonDecoder dec = new BsonDecoder();
        Map<String, Object> aDoc = dec.decodeDocument(bytes);
        assert (aDoc.equals(doc));
    }


    @Test
    public void mongoIdTest() throws Exception {
        List<MongoId> lst = new ArrayList<>();
        log.info("Creating...");
        for (int i = 0; i < 1000; i++) {
            MongoId id = new MongoId();
            assert (!lst.contains(id));
            lst.add(id);
        }
        log.info("done");
    }


    @Test
    public void encodeTest() throws Exception {
        Map<String, Object> doc = new HashMap<>();

        doc.put("_id", new MongoId());
        doc.put("counter", 123);
        doc.put("value", "a value");

        Map<String, Object> subDoc = new HashMap<>();
        subDoc.put("some", 1223.2);
        subDoc.put("bool", true);
        subDoc.put("created", new Date());

        doc.put("sub", subDoc);


        byte[] bytes = BsonEncoder.encodeDocument(doc);

        log.error(getHex(bytes));
        System.err.println(getHex(bytes));

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
