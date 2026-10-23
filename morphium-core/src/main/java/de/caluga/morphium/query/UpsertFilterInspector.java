package de.caluga.morphium.query;

import java.util.List;
import java.util.Map;

/**
 * Decides whether the {@code _id} of a document inserted by an upsert is already determined by the filter, so that
 * {@link Query#findOneAndUpdate(Map, boolean, boolean)} must not add a generated {@code $setOnInsert._id} (MongoDB would
 * answer such an update with code 66, "would modify the immutable field _id").
 * <p>
 * The rule was derived from a probe on a real MongoDB (docs/research/upsert-id-seeding-probe.md): the server seeds the
 * {@code _id} from an <em>equality</em> on {@code _id}, i.e. a plain value (also {@code null} or a literal document),
 * {@code $eq}, an {@code $in} with exactly one element, or a dotted {@code _id.x} key, reached through {@code $and} (at any
 * depth) and through an {@code $or} with a single branch. Multi-valued {@code $in}, an {@code $or} with several branches,
 * range operators, regular expressions and {@code $nor} do not seed.
 * <p>
 * A top-level {@code _id} key has always counted as pinned, whatever its predicate; that behaviour is kept.
 */
final class UpsertFilterInspector {
    private UpsertFilterInspector() {
    }

    static boolean pinsId(Map<String, Object> filter) {
        if (filter == null) {
            return false;
        }

        return filter.containsKey("_id") || seeds(filter);
    }

    private static boolean seeds(Map<?, ?> filter) {
        for (Map.Entry<?, ?> e : filter.entrySet()) {
            Object key = e.getKey();
            Object value = e.getValue();

            if (!(key instanceof String k)) {
                continue;
            }

            if (k.equals("_id")) {
                if (isEquality(value)) {
                    return true;
                }
            } else if (k.startsWith("_id.")) {
                return true;
            } else if (k.equals("$and") && value instanceof List<?> branches) {
                for (Object b : branches) {
                    if (b instanceof Map<?, ?> m && seeds(m)) {
                        return true;
                    }
                }
            } else if (k.equals("$or") && value instanceof List<?> branches && branches.size() == 1) {
                if (branches.get(0) instanceof Map<?, ?> m && seeds(m)) {
                    return true;
                }
            }
        }

        return false;
    }

    /** true if the predicate on _id is an equality the server seeds the id from */
    private static boolean isEquality(Object predicate) {
        if (predicate instanceof java.util.regex.Pattern) {
            return false;
        }

        if (predicate instanceof Map<?, ?> m) {
            boolean hasOperator = m.keySet().stream().anyMatch(k -> k instanceof String s && s.startsWith("$"));

            if (!hasOperator) {
                return true; // literal embedded document
            }

            if (m.containsKey("$eq")) {
                return true;
            }

            return m.get("$in") instanceof List<?> in && in.size() == 1;
        }

        return true; // plain value, also null
    }
}
