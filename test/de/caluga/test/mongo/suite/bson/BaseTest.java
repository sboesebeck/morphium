package de.caluga.test.mongo.suite.bson;/**
 * Created by stephan on 04.11.15.
 */

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;

/**
 * TODO: Add Documentation here
 **/
public class BaseTest {

    private static String[] chars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",};

    @org.junit.BeforeClass
    public static void setUpClass() throws Exception {
        System.setProperty("morphium.log.level", "4");
        System.setProperty("morphium.log.synced", "true");
        System.setProperty("morphium.log.file", "-");
        java.util.logging.Logger l = java.util.logging.Logger.getGlobal();
        l.setLevel(Level.SEVERE);
    }

    public String getHex(byte[] b) {
        return getHex(b, -1);
    }


    public String getHex(byte[] b, int sz) {
        StringBuilder sb = new StringBuilder();

        int mainIdx = 0;

        int end = b.length;
        if (sz > 0 && sz < b.length)
            end = sz;
        while (mainIdx < end) {
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

    public String getHex(byte by) {
        String ret = "";
        int idx = (by >>> 4) & 0x0f;
        ret += chars[idx];
        idx = by & 0x0f;
        ret += chars[idx];
        return ret;
    }
}
