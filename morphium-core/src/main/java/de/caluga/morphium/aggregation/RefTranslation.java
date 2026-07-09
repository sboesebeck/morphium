package de.caluga.morphium.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Internal helper for the opt-in translateAggregationFieldNames behavior: translates
 * $-field references (not $$-variables) from Java property names to Mongo field names,
 * recursing into maps and lists. Values that are no entity property pass through
 * unchanged (the supplied translation function is expected to be identity for unknown
 * names).
 */
public final class RefTranslation {

    private RefTranslation() {
    }

    @SuppressWarnings("unchecked")
    public static Object translateRefs(Object value, UnaryOperator<String> tf) {
        if (value instanceof String) {
            String s = (String) value;
            if (s.startsWith("$") && !s.startsWith("$$")) {
                return "$" + tf.apply(s.substring(1));
            }
            return s;
        }
        if (value instanceof Map) {
            Map<String, Object> ret = new HashMap<>();
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                ret.put(e.getKey(), translateRefs(e.getValue(), tf));
            }
            return ret;
        }
        if (value instanceof List) {
            List<Object> ret = new ArrayList<>();
            for (Object o : (List<Object>) value) {
                ret.add(translateRefs(o, tf));
            }
            return ret;
        }
        return value;
    }
}
