package de.caluga.morphium.aggregation.internal;

import de.caluga.morphium.Morphium;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Internal helper shared by AggregatorImpl and InMemAggregator: implements the opt-in
 * translateAggregationFieldNames behavior (issue #208/#217); the
 * untranslated-reference warning lives in UntranslatedRefWarner. Not intended as public API.
 *
 * <p>The effective flag value is snapshotted from MorphiumConfig when the aggregator is
 * created, so a pipeline is always built consistently even if the config changes
 * mid-build; the per-aggregator override still takes precedence at any time.</p>
 */
public final class FieldNameTranslation {

    private static final Logger log = LoggerFactory.getLogger(FieldNameTranslation.class);

    private final Morphium morphium;
    private final Class<?> searchType;
    private final boolean configEnabled;
    private Boolean override;

    public FieldNameTranslation(Morphium morphium, Class<?> searchType) {
        this.morphium = morphium;
        this.searchType = searchType;
        this.configEnabled = morphium != null
            && morphium.getConfig().objectMappingSettings().isTranslateAggregationFieldNames();
    }

    public void setOverride(Boolean override) {
        this.override = override;
    }

    public boolean isEnabled() {
        return override != null ? override : configEnabled;
    }

    /** the single implementation of Java-property-name to Mongo-field-name translation;
     * null-safe: without a Morphium instance or type, and for names that are no entity
     * property, the name passes through unchanged */
    public static String translate(Morphium morphium, Class<?> type, String field) {
        if (morphium == null || type == null) return field;
        if (morphium.getARHelper().getField(type, field) == null) return field;
        return morphium.getARHelper().getMongoFieldName(type, field);
    }

    /** translates a Java property name of the search type to its Mongo field name;
     * names that are no property pass through unchanged */
    public String tf(String field) {
        return tf(searchType, field);
    }

    /** same as {@link #tf(String)}, but against an explicitly given entity type
     * (e.g. the from type of a lookup/graphLookup) */
    public String tf(Class<?> type, String field) {
        return translate(morphium, type, field);
    }

    /** returns a copy of the map with all keys translated, preserving order */
    public <V> Map<String, V> translateKeys(Map<String, V> m) {
        Map<String, V> ret = new LinkedHashMap<>();
        for (Map.Entry<String, V> e : m.entrySet()) {
            String key = tf(e.getKey());
            if (ret.put(key, e.getValue()) != null) {
                log.warn("Field-name translation collapsed two keys onto '{}' - one entry was silently replaced", key);
            }
        }
        return ret;
    }

    /** translates $-refs against the search type, see {@link #translateRefs(Object, UnaryOperator)} */
    public Object translateRefs(Object value) {
        return translateRefs(value, this::tf);
    }

    /**
     * translates $-field references (not $$-variables) from Java property names to Mongo
     * field names, recursing into maps and lists. Dot-paths translate their first
     * segment ($itemCount.sub becomes $item_count.sub); deeper segments cannot be
     * resolved without embedded-type information and pass through unchanged.
     * $literal subtrees are data by definition and are never touched. Note that this
     * operates on the serialized pipeline: Expr.string("$...") is indistinguishable
     * from a field reference here and is therefore not supported with the flag on
     * (wrap in $literal instead).
     */
    @SuppressWarnings("unchecked")
    public static Object translateRefs(Object value, UnaryOperator<String> tf) {
        if (value instanceof String) {
            String s = (String) value;
            if (s.startsWith("$") && !s.startsWith("$$")) {
                String path = s.substring(1);
                int dot = path.indexOf('.');
                if (dot < 0) {
                    return "$" + tf.apply(path);
                }
                return "$" + tf.apply(path.substring(0, dot)) + path.substring(dot);
            }
            return s;
        }
        if (value instanceof Map) {
            Map<String, Object> ret = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                if ("$literal".equals(e.getKey())) {
                    ret.put(e.getKey(), e.getValue());
                } else {
                    ret.put(e.getKey(), translateRefs(e.getValue(), tf));
                }
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
