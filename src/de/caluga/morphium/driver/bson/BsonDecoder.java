package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 29.10.15.
 */

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * decoding BSON coming from mongodb
 **/
@SuppressWarnings("WeakerAccess")
public class BsonDecoder {

    public Map<String, Object> decodeDocument(byte[] in) throws UnsupportedEncodingException {
        Map<String, Object> ret = new HashMap<>();
        decodeDocumentIn(ret, in, 0);
        return ret;
    }

    public int decodeDocumentIn(Map<String, Object> ret, byte[] in, int startIndex) throws UnsupportedEncodingException {
        int sz = readInt(in, startIndex);
        if (sz > in.length) {
            throw new RuntimeException("error - size differs! read " + sz + " but buffer is " + in.length);
        }

        for (int idx = startIndex + 4; startIndex - 4 - idx < sz; ) {
            String name;
            byte type = in[idx++];
            if (type == 0) {
                break; //end of document
            }

            int l = 0;
            while (in[idx + l] != 0) {
                l++;
            }
            name = new String(in, idx, l, "UTF-8");
            idx += l + 1; //trailling 0

            Object value;
            switch (type) {
                case 0x01:
                    //double
                    long lng = readLong(in, idx);
                    value = Double.longBitsToDouble(lng);
                    idx += 8;
                    break;
                case 0x02:
                    //string
                    int strlen = readInt(in, idx);
                    value = new String(in, idx + 4, strlen - 1, "UTF-8");
                    idx += strlen + 4;
                    break;
                case 0x03:
                    //document
                    Map<String, Object> doc = new HashMap<>();
                    int len = decodeDocumentIn(doc, in, idx);
                    value = doc;
                    idx += len;
                    break;
                case 0x04:
                    //array
                    doc = new HashMap<>();
                    len = decodeDocumentIn(doc, in, idx);
                    List<Object> lst = new ArrayList<>();

                    for (int i = 0; i < doc.size(); i++) {
                        lst.add(doc.get("" + i));
                    }
                    value = lst;
                    idx += len;
                    break;

                case 0x05:
                    //binary data
                    //                    throw new RuntimeException("Not implemented yet!");
                    int boblen = readInt(in, idx);
                    byte[] bobdata = new byte[boblen];
                    //skipping subtype!
                    System.arraycopy(in, idx + 5, bobdata, 0, boblen);
                    MongoBob bob = new MongoBob(bobdata);
                    idx += boblen + 5;
                    value = bob;
                    break;
                case 0x0e:
                    //deprecated
                case 0x0c:
                    //pointer - deprecated
                case 0x06:
                    //undefined / deprecated
                    throw new RuntimeException("deprecated type detected!");
                case 0x07:
                    //MongoId
                    value = new MorphiumId(in, idx);
                    idx += 12;
                    break;
                case 0x08:
                    //boolean
                    value = in[idx] == 0x01;
                    idx++;
                    break;
                case 0x09:
                    //Datetime
                    lng = readLong(in, idx);
                    idx += 8;
                    value = new Date(lng);
                    break;
                case 0x0a:
                    //null
                    value = null;
                    break;
                case 0x0b:
                    //regex
                    l = 0;
                    while (in[idx + l] != 0) {
                        l++;
                    }
                    //                    String pattern = new String(in, idx, l, "UTF-8");
                    idx += l;

                    l = 0;
                    while (in[idx + l] != 0) {
                        l++;
                    }
                    String opts = new String(in, idx, l, "UTF-8");
                    idx += l;
                    int flags = 0;
                    if (opts.contains("i")) {
                        flags = flags | Pattern.CASE_INSENSITIVE;
                    }
                    if (opts.contains("m")) {
                        flags = flags | Pattern.MULTILINE;
                    }
                    if (opts.contains("l")) {
                        flags = flags | Pattern.LITERAL;
                    }

                    if (opts.contains("s")) {
                        flags = flags | Pattern.DOTALL;
                    }

                    if (opts.contains("u")) {
                        flags = flags | Pattern.UNICODE_CASE;
                    }
                    value = Pattern.compile(name, flags);
                    break;
                case 0x0d:
                    //javascript
                    strlen = readInt(in, idx);
                    String code = new String(in, idx + 4, strlen - 1, "UTF-8");
                    value = new MongoJSScript(code);
                    idx += strlen + 4;
                    break;
                case 0x0f:
                    //javascript w/ scope
                    //first 4 bytes the whole length
                    strlen = readInt(in, idx + 4);
                    code = new String(in, idx + 8, strlen - 1, "UTF-8");
                    Map<String, Object> scope = new HashMap<>();
                    int doclen = decodeDocumentIn(scope, in, idx + 8 + strlen);
                    value = new MongoJSScript(code, scope);
                    idx += doclen + 8 + strlen;
                    break;
                case 0x10:
                    //32 bit int
                    value = readInt(in, idx);
                    idx += 4;
                    break;
                case 0x11:
                    //timestamp - internal
                    //                    throw new RuntimeException("Got internaltimestamp");
                case 0x12:
                    //64 bit long
                    value = readLong(in, idx);
                    idx += 8;
                    break;
                case (byte) 0xff:
                    //min key
                    value = new MongoMinKey();
                    break;
                case 0x7f:
                    //max key
                    //noinspection UnusedAssignment
                    value = new MongoMaxKey();
                default:
                    throw new RuntimeException("unknown data type: " + in[idx]);

            }
            ret.put(name, value);
        }
        return sz;
    }


    public int readInt(byte[] bytes, int idx) {
        return (bytes[idx] & 0xFF) | (bytes[idx + 1] & 0xFF) << 8 | (bytes[idx + 2] & 0xFF) << 16 | ((bytes[idx + 3] & 0xFF) << 24);

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
}
