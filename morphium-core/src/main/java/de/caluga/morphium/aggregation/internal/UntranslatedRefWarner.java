package de.caluga.morphium.aggregation.internal;

import de.caluga.morphium.aggregation.Expr;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Internal helper shared by AggregatorImpl and InMemAggregator: project(Map) translates
 * its keys through the entity's field-name mapping. If a later stage references such a
 * key by the name the user wrote, the reference points at a non-existent field and
 * MongoDB silently returns 0 / [] (issue #208). This check makes that case visible with
 * a WARN. Not intended as public API.
 */
public final class UntranslatedRefWarner {

    private final Map<String, String> translatedProjectKeys = new LinkedHashMap<>();
    private final Set<String> warnedRefs = new HashSet<>();

    /** project(Map) reports every key it renamed here, so later stages referencing the
     * original spelling can be warned about */
    public void recordProjectKeyTranslation(String original, String translated) {
        translatedProjectKeys.put(original, translated);
    }

    /**
     * scans a pipeline stage for $-references to a project key that was silently
     * renamed and logs a WARN for every distinct hit. $$-variables and $literal
     * subtrees (data by definition) are ignored; dot-paths are matched by their
     * first segment.
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
                             + "Reference '${}' instead.",
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
