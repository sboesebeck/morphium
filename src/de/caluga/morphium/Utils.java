package de.caluga.morphium;/**
 * Created by stephan on 25.11.15.
 */

import de.caluga.morphium.driver.MorphiumId;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Utility class
 **/
@SuppressWarnings("WeakerAccess")
public class Utils {

    public static final String[] hexChars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",};

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public static String toJsonString(Object o) {
        if (o == null) return "null";
        StringBuilder b = new StringBuilder();
        boolean comma = false;
        if (o instanceof Collection) {
            b.append(" [ ");

            for (Object obj : ((Collection) o)) {
                if (comma) {
                    b.append(", ");
                }
                comma = true;
                b.append(toJsonString(obj));
            }
            b.append("]");
            return b.toString();
        } else if ((o instanceof String) || (o instanceof MorphiumId) || (o.getClass().isEnum())) {
            return "\"" + o.toString() + "\"";
        } else if (!(o instanceof Map)) {
            return o.toString();
        }
        Map<String, Object> db = (Map<String, Object>) o;

        b.append("{ ");
        comma = false;
        for (Map.Entry<String, Object> e : db.entrySet()) {
            if (comma) {
                b.append(", ");
            }

            comma = true;
            b.append("\"");
            b.append(e.getKey());
            b.append("\"");

            b.append(" : ");
            if (e.getValue() == null) {
                b.append(" null");
            } else if (e.getValue() instanceof String) {
                b.append("\"");
                b.append(e.getValue());
                b.append("\"");
            } else if (e.getValue().getClass().isEnum()) {
                b.append("\"");
                b.append(e.getValue().toString());
                b.append("\"");
            } else {
                b.append(toJsonString(e.getValue()));
            }

        }
        b.append(" } ");
        return b.toString();
    }

    public static <K, V> Map<K, V> getMap(K key, V value) {
        LinkedHashMap<K, V> ret = new LinkedHashMap<>();
        ret.put(key, value);
        return ret;
    }

    public static Map<String, Integer> getIntMap(String key, Integer value) {
        HashMap<String, Integer> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }


    public static String getHex(long i) {
        return (getHex((byte) (i >> 56 & 0xff)) + getHex((byte) (i >> 48 & 0xff)) + getHex((byte) (i >> 40 & 0xff)) + getHex((byte) (i >> 32 & 0xff)) + getHex((byte) (i >> 24 & 0xff)) + getHex((byte) (i >> 16 & 0xff)) + getHex((byte) (i >> 8 & 0xff)) + getHex((byte) (i & 0xff)));
    }

    public static String getHex(int i) {
        return (getHex((byte) (i >> 24 & 0xff)) + getHex((byte) (i >> 16 & 0xff)) + getHex((byte) (i >> 8 & 0xff)) + getHex((byte) (i & 0xff)));
    }

    public static String getHex(byte[] b) {
        return getHex(b, -1);
    }


    public static String getHex(byte[] b, int sz) {
        StringBuilder sb = new StringBuilder();

        int mainIdx = 0;

        int end = b.length;
        if (sz > 0 && sz < b.length) {
            end = sz;
        }
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
            String str = new String(sr, 0, l, StandardCharsets.UTF_8);
            sb.append("    ");
            sb.append(str);
            sb.append("\n");
            mainIdx += 16;
        }
        return sb.toString();

    }

    public static String getHex(byte by) {
        String ret = "";
        int idx = (by >>> 4) & 0x0f;
        ret += hexChars[idx];
        idx = by & 0x0f;
        ret += hexChars[idx];
        return ret;
    }

    public static String getCacheKey(Class type, Map<String, Object> qo, Map<String, Integer> sort, Map<String, Object> projection, String collection, int skip, int limit, AnnotationAndReflectionHelper anHelper) {

        StringBuilder b = new StringBuilder();
        b.append(qo.toString());
        b.append(" c:").append(collection);
        b.append(" l:");
        b.append(limit);
        b.append(" s:");
        b.append(skip);
        if (sort != null) {
            b.append(" sort:{");
            for (Map.Entry<String, Integer> s : sort.entrySet()) {
                b.append(" ").append(s.getKey()).append(":").append(s.getValue());
            }
            b.append("}");
        }
        if (projection != null) {
            List<Field> fields = anHelper.getAllFields(type);
            boolean addProjection = false;
            if (projection.size() == fields.size()) {
                for (Field f : fields) {
                    if (!projection.containsKey(anHelper.getFieldName(type, f.getName()))) {
                        addProjection = true;
                        break;
                    }
                }
            } else {
                addProjection = true;
            }

            if (addProjection) {
                b.append(" project:{");
                for (Map.Entry<String, Object> s : projection.entrySet()) {
                    b.append(" ").append(s.getKey()).append(":").append(s.getValue());
                }
                b.append("}");
            }
        }
        return b.toString();
    }


    public static Object replaceMorphiumIds(Map m) {
        Map toSet = new LinkedHashMap();
        for (Map.Entry e : (Set<Map.Entry>) m.entrySet()) {
            if (e.getKey().equals("morphium id")) {
                //identifier!
                return new MorphiumId(e.getValue().toString());
            } else if (e.getValue() instanceof Map) {
                toSet.put(e.getKey(), replaceMorphiumIds((Map) e.getValue()));
            } else if (e.getValue() instanceof Collection) {
                toSet.put(e.getKey(), replaceMorphiumIds((Collection) e.getValue()));
            } else {
                toSet.put(e.getKey(), e.getValue());
            }
        }
        return toSet;
    }

    public static Collection replaceMorphiumIds(Collection value) {
        Collection ret = new ArrayList();
        for (Object o : value) {
            if (o instanceof Map && ((Map) o).containsKey("morphium id")) {
                ret.add(new MorphiumId((String) ((Map) o).get("morphium id")));
            } else if (o instanceof Map) {
                ret.add(replaceMorphiumIds((Map) o));
            } else {
                ret.add(o);
            }
        }
        return ret;
    }

}
