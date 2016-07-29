package de.caluga.morphium.driver.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * User: Stephan Bösebeck
 * Date: 26.10.15
 * Time: 22:44
 * <p>
 * encoding BSON for sending data do mongodb
 */
@SuppressWarnings("WeakerAccess")
public class BsonEncoder {
    private final ByteArrayOutputStream out;

    public BsonEncoder() {

        out = new ByteArrayOutputStream();
    }

    public static byte[] encodeDocument(Map<String, Object> m) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            BsonEncoder enc = new BsonEncoder();
            enc.encodeObject(e.getKey(), e.getValue());
            try {
                o.write(enc.getBytes());
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        ByteArrayOutputStream o2 = new ByteArrayOutputStream();
        for (int i = 3; i >= 0; i--) o2.write((byte) ((o.size() + 4 + 1 >> ((7 - i) * 8)) & 0xff));
        try {
            o2.write(o.toByteArray());
            o2.write(0x00);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return o2.toByteArray();
    }

    @SuppressWarnings("UnusedReturnValue")
    private BsonEncoder string(String s) {
        try {
            byte[] b = s.getBytes("UTF-8");
            writeInt(b.length + 1);
            writeBytes(b);
            out.write((byte) 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    private BsonEncoder cString(String s) {
        try {
            byte[] b = s.getBytes("UTF-8");
            writeBytes(b);
            out.write((byte) 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    public byte[] getBytes() {
        //        ByteArrayOutputStream n = new ByteArrayOutputStream();
        ////        int sz = out.size() + 5; //4 + terminating 0
        ////        for (int i = 0; i < 4; i++) n.write((byte) ((sz >> ((7 - i) * 8)) & 0xff));
        //        try {
        //            n.write(out.toByteArray());
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        }
        //        n.write(0x00);
        //        return n.toByteArray();
        return out.toByteArray();
    }

    @SuppressWarnings("UnusedReturnValue")
    private BsonEncoder encodeObject(String n, Object v) {

        if (v == null) {
            writeByte(10).cString(n);

        } else if (v instanceof Double) {
            writeByte(1).cString(n);
            long lng = Double.doubleToLongBits((Double) v);

            writeLong(lng);
        } else if (v instanceof String) {
            writeByte(2);
            cString(n);
            string((String) v);
        } else if (v instanceof List || List.class.isAssignableFrom(v.getClass()) || Collection.class.isAssignableFrom(v.getClass())) {
            writeByte(4);
            cString(n);

            Map<String, Object> doc = new HashMap<>();
            int cnt = 0;
            if (Collection.class.isAssignableFrom(v.getClass())) {
                List l = new ArrayList();
                //noinspection unchecked
                l.addAll((Collection) v);
                v = l;

            }
            //noinspection ConstantConditions
            for (Object o : (List) v) {
                //cString(""+(cnt++));
                //                encodeObject("" + (cnt++), o);
                doc.put("" + (cnt++), o);
            }

            writeBytes(BsonEncoder.encodeDocument(doc));

        } else if (v instanceof Map || Map.class.isAssignableFrom(v.getClass())) {
            writeByte(3);
            cString(n);
            @SuppressWarnings({"unchecked", "ConstantConditions"}) byte[] b = BsonEncoder.encodeDocument(((Map<String, Object>) v));
            writeBytes(b);
        } else if (v instanceof MongoBob) {
            //binary data
            writeByte(5);
            cString(n);
            MongoBob b = (MongoBob) v;
            byte[] data = b.getData();
            if (data == null) {
                data = new byte[0];
            }
            writeInt(data.length);
            writeByte(0); //subtype

            writeBytes(data);
        } else if (MorphiumId.class.isAssignableFrom(v.getClass())) {
            writeByte(7);
            cString(n);
            writeBytes(((MorphiumId) v).getBytes());

        } else if (v.getClass().isAssignableFrom(Boolean.class)) {
            boolean b = (Boolean) v;
            writeByte(8);
            cString(n);
            if (b) {
                writeByte(1);
            } else {
                writeByte(0);
            }
        } else if (v.getClass().isAssignableFrom(Date.class)) {
            writeByte(9);
            cString(n);
            writeLong(((Date) v).getTime());
        } else if (v.getClass().isAssignableFrom(Calendar.class)) {
            writeByte(9);
            cString(n);
            writeLong(((Calendar) v).getTimeInMillis());
        } else if (v.getClass().isAssignableFrom(Pattern.class)) {
            Pattern p = (Pattern) v;
            String flags = "";
            int f = p.flags();
            if ((f & Pattern.MULTILINE) != 0) {
                flags += "m";
            } else if ((f & Pattern.CASE_INSENSITIVE) != 0) {
                flags += "i";
            } else if ((f & Pattern.DOTALL) != 0) {
                flags += "s";
            }

            writeByte(0x0b);
            cString(n);
            cString(p.pattern());
            cString(flags);
        } else if (v.getClass().isAssignableFrom(MongoJSScript.class)) {
            ///with w/ scope 0xf, otherwise 0xd
            MongoJSScript s = (MongoJSScript) v;
            if (s.getContext() != null) {
                //                try {
                    writeByte(0x0f);
                //                    long sz = n.getBytes("UTF-8").length + 1 + 4 + b.length; //size+stringlength+1 (ending 0)+document length
                cString(n);
                int l = s.getJs().length() + 4; //String length
                byte[] b = BsonEncoder.encodeDocument(s.getContext());
                l = l + b.length;
                writeByte(l);
                string(s.getJs());
                writeBytes(b);
                //                } catch (IOException e) {
                //                    e.printStackTrace();
                //                }

            } else {
                writeByte(0x0d);
                cString(n);
                string(s.getJs());

            }
        } else if (v.getClass().isAssignableFrom(Integer.class)) {
            writeByte(0x10);
            cString(n);
            int val = (Integer) v;
            writeInt(val);
        } else if (v.getClass().isAssignableFrom(Long.class)) {
            writeByte(0x12);
            cString(n);
            long val = (Long) v;
            writeLong(val);
        } else if (v.getClass().isAssignableFrom(MongoMinKey.class)) {
            writeByte(0xff);
            cString(n);
        }
        return this;
    }

    private void writeBytes(byte[] data) {
        try {
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeInt(int val) {
        for (int i = 3; i >= 0; i--) writeByte((byte) ((val >> ((7 - i) * 8)) & 0xff));
    }

    private void writeLong(long lng) {
        for (int i = 7; i >= 0; i--) writeByte((byte) ((lng >> ((7 - i) * 8)) & 0xff));
    }

    private BsonEncoder writeByte(int v) {
        out.write((byte) v);
        return this;
    }
}
