package de.caluga.morphium.data;

import de.caluga.morphium.query.ReturnDocument;

import java.util.ArrayList;
import java.util.List;

/**
 * The content of an {@link AtomicUpdate} in a form that survives the trip through generated code: the Quarkus build encodes it into a string constant, the
 * Spring proxy builds it from the annotation. Each assignment is {@code field=param} ({@code field=:param}) or {@code field=literal}.
 */
public record AtomicUpdateSpec(String where, List<String> set, List<String> setOnInsert, List<String> inc, boolean upsert, boolean many, ReturnDocument returns,
                               String orderBy, String paramMapSpec) {

    private static final char FIELDS = '\u001F';
    private static final char ITEMS = '\u001E';

    /** an assignment as the spec holds it: {@code field=:param} or {@code field=literal} */
    public static String assignment(String field, String param, String value) {
        boolean hasParam = param != null && !param.isEmpty();
        boolean hasValue = value != null && !value.isEmpty();

        if (hasParam == hasValue) {
            throw new IllegalArgumentException("@Assign(field = \"" + field + "\") needs exactly one of param and value");
        }

        return field + "=" + (hasParam ? ":" + param : value);
    }

    public String encode() {
        return String.join(String.valueOf(FIELDS), where, String.join(String.valueOf(ITEMS), set), String.join(String.valueOf(ITEMS), setOnInsert),
                String.join(String.valueOf(ITEMS), inc), String.valueOf(upsert), String.valueOf(many), returns.name(), orderBy, paramMapSpec);
    }

    public static AtomicUpdateSpec decode(String encoded) {
        String[] f = encoded.split(String.valueOf(FIELDS), -1);
        return new AtomicUpdateSpec(f[0], items(f[1]), items(f[2]), items(f[3]), Boolean.parseBoolean(f[4]), Boolean.parseBoolean(f[5]), ReturnDocument.valueOf(f[6]), f[7], f[8]);
    }

    private static List<String> items(String s) {
        List<String> items = new ArrayList<>();

        if (!s.isEmpty()) {
            items.addAll(List.of(s.split(String.valueOf(ITEMS), -1)));
        }

        return items;
    }

    /** the two forms of an {@link AtomicUpdate}, chosen by the return type */
    public enum Form { COUNT, DOCUMENT }

    /**
     * Checks the spec against the method and the entity, for the Quarkus build and the creation of the Spring proxy.
     *
     * @param method      what the messages name, for example {@code Leases.tryAcquire}
     * @param paramNames  the names of the method parameters that can be bound ({@code @Param} or the parameter name)
     * @param entityFields the fields of the entity; null or empty skips the field check
     * @throws IllegalStateException with a message that names the method
     */
    public void validate(String method, Form form, java.util.Set<String> paramNames, java.util.Set<String> entityFields) {
        String prefix = "@AtomicUpdate on " + method + ": ";

        if (set.isEmpty() && setOnInsert.isEmpty() && inc.isEmpty()) {
            throw new IllegalStateException(prefix + "needs at least one assignment in set, setOnInsert or inc");
        }

        if (!setOnInsert.isEmpty() && !upsert) {
            throw new IllegalStateException(prefix + "setOnInsert only does something with upsert = true");
        }

        if (form == Form.COUNT && orderBy != null && !orderBy.isEmpty()) {
            throw new IllegalStateException(prefix + "@OrderBy only belongs on the find-and-modify form");
        }

        if (form == Form.DOCUMENT && many) {
            throw new IllegalStateException(prefix + "many = true only belongs on the count form; the find-and-modify form changes one document");
        }

        checkAssignments(prefix, "set", set, paramNames, entityFields, false);
        checkAssignments(prefix, "setOnInsert", setOnInsert, paramNames, entityFields, false);
        checkAssignments(prefix, "inc", inc, paramNames, entityFields, true);

        String condition = where == null ? "" : where.strip();
        String jdql = condition.isEmpty() ? "" : condition.toUpperCase(java.util.Locale.ROOT).startsWith("WHERE ") ? condition : "WHERE " + condition;

        String operandProblem;

        try {
            operandProblem = JdqlOperands.check(jdql);
        } catch (RuntimeException e) {
            throw new IllegalStateException(prefix + "the condition \"" + where + "\" is not valid JDQL: " + e.getMessage(), e);
        }

        if (operandProblem != null) {
            throw new IllegalStateException(prefix + "the condition \"" + where + "\" is not valid JDQL: " + operandProblem);
        }

        if (JdqlOrdinalParameters.has(jdql)) {
            throw new IllegalStateException(prefix + "the condition uses ordinal parameters (?1); use named parameters (:name)");
        }

        java.util.regex.Matcher named = java.util.regex.Pattern.compile("(?<![A-Za-z0-9_']):([A-Za-z_][A-Za-z0-9_]*)").matcher(jdql);

        while (named.find()) {
            if (!paramNames.contains(named.group(1))) {
                throw new IllegalStateException(prefix + "the condition uses :" + named.group(1) + ", but the method has no parameter of that name "
                    + "(@Param(\"" + named.group(1) + "\") or the parameter name with -parameters); it has " + paramNames);
            }
        }
    }

    private static void checkAssignments(String prefix, String attribute, List<String> items, java.util.Set<String> paramNames, java.util.Set<String> entityFields, boolean numeric) {
        for (String item : items) {
            int eq = item.indexOf('=');
            String field = item.substring(0, eq);
            String value = item.substring(eq + 1);
            String root = field.contains(".") ? field.substring(0, field.indexOf('.')) : field;

            if (entityFields != null && !entityFields.isEmpty() && !entityFields.contains(root)) {
                throw new IllegalStateException(prefix + attribute + " names the field " + field + ", which the entity does not have (fields: " + entityFields + ")");
            }

            if (value.startsWith(":")) {
                if (!paramNames.contains(value.substring(1))) {
                    throw new IllegalStateException(prefix + attribute + " for " + field + " uses the parameter " + value.substring(1) + ", but the method has no parameter of that name "
                        + "(@Param(\"" + value.substring(1) + "\") or the parameter name with -parameters); it has " + paramNames);
                }
            } else {
                Object literal;

                try {
                    literal = JdqlUpdate.literal(value);
                } catch (RuntimeException e) {
                    throw new IllegalStateException(prefix + attribute + " for " + field + ": the literal " + value + " is not valid (a number, a string in single quotes, TRUE or FALSE)", e);
                }

                if (numeric && !(literal instanceof Number)) {
                    throw new IllegalStateException(prefix + attribute + " for " + field + ": the literal " + value + " is not a number");
                }
            }
        }
    }
}
