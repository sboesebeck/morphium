package de.caluga.morphium.aggregation.internal;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Expr;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Internal helper shared by AggregatorImpl and InMemAggregator: implements the opt-in
 * translateAggregationFieldNames behavior and the untranslated-reference warning
 * (issue #208/#217). Not intended as public API.
 *
 * <p>The effective flag value is snapshotted from MorphiumConfig when the aggregator is
 * created, so a pipeline is always built consistently even if the config changes
 * mid-build; the per-aggregator override still takes precedence at any time.</p>
 */
public final class FieldNameTranslation {

    private final Morphium morphium;
    private final Class<?> searchType;
    private final boolean configEnabled;
    private Boolean override;
    private final Map<String, String> translatedProjectKeys = new LinkedHashMap<>();
    private final Set<String> warnedRefs = new HashSet<>();

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
            ret.put(tf(e.getKey()), e.getValue());
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

    /** project(Map) reports every key it renamed here, so later stages referencing the
     * original spelling can be warned about */
    public void recordProjectKeyTranslation(String original, String translated) {
        translatedProjectKeys.put(original, translated);
    }

    /**
     * scans a pipeline stage for $-references to a project key that was silently
     * renamed and logs a WARN for every distinct hit (issue #208: such references
     * yield 0 / [] without any error). $$-variables are ignored.
     */
    public void warnUntranslatedRefs(Object stage, Logger log) {
        if (translatedProjectKeys.isEmpty()) {
            return;
        }
        scan(stage, log);
    }

    @SuppressWarnings("unchecked")
    private void scan(Object value, Logger log) {
        if (value instanceof Expr) {
            // the InMemAggregator keeps Expr objects in its stages - scan their serialized form
            scan(((Expr) value).toQueryObject(), log);
            return;
        }
        if (value instanceof String) {
            String s = (String) value;
            if (s.startsWith("$") && !s.startsWith("$$")) {
                String firstSegment = s.substring(1).split("\\.")[0];
                String translated = translatedProjectKeys.get(firstSegment);
                if (translated != null && warnedRefs.add(firstSegment)) {
                    log.warn("Aggregation stage references '${}', but project(Map) translated that key to '{}'. "
                             + "The reference will not resolve and silently yields 0 / []. "
                             + "Reference '${}' instead, or enable translateAggregationFieldNames.",
                             firstSegment, translated, translated);
                }
            }
        } else if (value instanceof Map) {
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                if (!"$literal".equals(e.getKey())) {
                    scan(e.getValue(), log);
                }
            }
        } else if (value instanceof List) {
            for (Object v : (List<Object>) value) {
                scan(v, log);
            }
        }
    }
}
