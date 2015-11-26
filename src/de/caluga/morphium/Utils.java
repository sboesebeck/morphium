package de.caluga.morphium;/**
 * Created by stephan on 25.11.15.
 */

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class Utils {

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public static String toJsonString(Object o) {

        StringBuilder b = new StringBuilder();
        boolean comma = false;
        if (o instanceof Collection) {
            b.append(" [ ");

            for (Object obj : ((Collection) o)) {
                if (comma) b.append(", ");
                comma = true;
                b.append(toJsonString(obj));
            }
            b.append("]");
            return b.toString();
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

    public static Map<String, Object> getMap(String key, Object value) {
        HashMap<String, Object> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }

    public static Map<String, Integer> getIntMap(String key, Integer value) {
        HashMap<String, Integer> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }

}
