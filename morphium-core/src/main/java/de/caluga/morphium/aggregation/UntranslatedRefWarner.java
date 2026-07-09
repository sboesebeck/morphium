package de.caluga.morphium.aggregation;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Internal helper: project(Map) translates its keys through the entity's field-name
 * mapping. If a later stage references such a key by the name the user wrote, the
 * reference points at a non-existent field and MongoDB silently returns 0 / [] (see
 * issue #208). This check makes that case visible with a WARN.
 */
public final class UntranslatedRefWarner {

    private UntranslatedRefWarner() {
    }

    /**
     * scans a pipeline stage for $-references to a project key that was translated and
     * logs a WARN for every hit. $$-variables are ignored.
     *
     * @param translatedKeys original project key -&gt; translated Mongo field name
     * @param stage          the stage (or part of it) to scan
     * @param log            logger of the calling aggregator
     */
    public static void warnOnUntranslatedRefs(Map<String, String> translatedKeys, Object stage, Logger log) {
        if (translatedKeys == null || translatedKeys.isEmpty()) {
            return;
        }
        scan(translatedKeys, stage, log);
    }

    @SuppressWarnings("unchecked")
    private static void scan(Map<String, String> translatedKeys, Object value, Logger log) {
        if (value instanceof String) {
            String s = (String) value;
            if (s.startsWith("$") && !s.startsWith("$$")) {
                String firstSegment = s.substring(1).split("\\.")[0];
                String translated = translatedKeys.get(firstSegment);
                if (translated != null) {
                    log.warn("Aggregation stage references '${}', but project(Map) translated that key to '{}'. "
                             + "The reference will not resolve and silently yields 0 / []. "
                             + "Reference '${}' instead, or enable translateAggregationFieldNames.",
                             firstSegment, translated, translated);
                }
            }
        } else if (value instanceof Map) {
            for (Object v : ((Map<String, Object>) value).values()) {
                scan(translatedKeys, v, log);
            }
        } else if (value instanceof List) {
            for (Object v : (List<Object>) value) {
                scan(translatedKeys, v, log);
            }
        }
    }
}
