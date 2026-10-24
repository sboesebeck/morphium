package de.caluga.morphium.data;

import de.caluga.morphium.data.QueryDescriptor.Combinator;
import de.caluga.morphium.data.QueryDescriptor.Condition;
import de.caluga.morphium.query.ReturnDocument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Derived update methods (MOR-022), a Morphium extension of Jakarta Data (the specification has no derived update; its {@code UPDATE} exists as a statement of {@code @Query}):
 *
 * <pre>{@code
 * long updateByStatus(String status, int priority, Instant seenAt);                 // WHERE status = :status, then $set priority and seenAt
 * long updateByStatusAndOwner(String status, String owner, boolean archived);
 * Optional<Job> updateByIdAndState(String id, String state, String owner);          // the find-and-modify form: the document after the update
 * }</pre>
 *
 * The part after {@code updateBy} is parsed like the one of a {@code deleteBy} method: the conditions consume the first parameters in order. <b>Every parameter after them is
 * a value to set, and its name is the field it sets</b> (the name of the parameter with {@code -parameters}, or {@code @Param}). One update command with one {@code $set} is
 * issued, through the same path as {@link AtomicUpdate}: field names are translated, values are marshalled, {@code @Version} is incremented and {@code @LastChange} is set.
 *
 * <p>The return type chooses the form like for {@link AtomicUpdate}: {@code void}, {@code boolean}, {@code int} or {@code long} update every matching document and give the
 * count; the entity or an {@code Optional} of it changes one document and returns it as it is after the update. A method without a parameter to set is refused, as are the
 * conditions that a JDQL {@code WHERE} cannot say ({@code StartsWith}, {@code IgnoreCase}, ...); use {@link AtomicUpdate} or {@code @Query("UPDATE ...")} for those.
 */
public final class DerivedUpdate {

    private static final Pattern NAME = Pattern.compile("^updateBy(.+)$");

    private DerivedUpdate() {
    }

    /** whether the method name is one of a derived update ({@code updateBy...}); the plain {@code update} is the lifecycle method of Jakarta Data and no derived update */
    public static boolean isDerivedUpdateName(String methodName) {
        return methodName != null && NAME.matcher(methodName).matches();
    }

    /**
     * The update that a derived update method stands for.
     *
     * @param methodName    for example {@code updateByStatus}
     * @param paramMapSpec  the parameters as {@code name:index,name:index} (the format of {@link AtomicUpdateSpec#paramMapSpec()}), in the order of the method
     * @param countForm     true for {@code void}, {@code boolean}, {@code int} and {@code long}; false for the entity and {@code Optional}
     * @param entityFields  the Java field names of the entity, null or empty skips the check of the names
     * @throws IllegalStateException with a message that names the method
     */
    public static AtomicUpdateSpec derive(String methodName, String paramMapSpec, boolean countForm, Set<String> entityFields) {
        Matcher m = NAME.matcher(methodName);

        if (!m.matches()) {
            throw new IllegalStateException(methodName + " is not a derived update method (updateBy...)");
        }

        List<String> names = parameterNames(paramMapSpec);
        QueryDescriptor criteria;

        try {
            criteria = MethodNameParser.parse("deleteBy" + m.group(1), entityFields);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(methodName + ": " + e.getMessage(), e);
        }

        if (!criteria.orderBy().isEmpty()) {
            throw new IllegalStateException(methodName + ": OrderBy does not belong in the name of an updateBy method; use @AtomicUpdate with @OrderBy for the find-and-modify form");
        }

        int used = 0;
        List<String> terms = new ArrayList<>();

        for (Condition c : criteria.conditions()) {
            terms.add(term(methodName, c, names));
            used = Math.max(used, Math.max(c.paramIndex(), c.paramIndex2()) + 1);
        }

        if (used >= names.size()) {
            throw new IllegalStateException(methodName + ": no parameter is left to set. The conditions of the name take the first " + used + " parameter(s); every parameter after them is a value "
                    + "to set, and its name is the field it sets (updateByStatus(String status, int priority))");
        }

        List<String> set = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (int i = used; i < names.size(); i++) {
            String field = names.get(i);

            if (field == null || field.matches("arg\\d+")) {
                throw new IllegalStateException(methodName + ": parameter " + (i + 1) + " has no usable name (" + field + "), and the name of a value parameter is the field it sets; "
                        + "compile with -parameters or name it with @Param");
            }

            if (!seen.add(field)) {
                throw new IllegalStateException(methodName + ": the field " + field + " is set twice");
            }

            set.add(AtomicUpdateSpec.assignment(field, field, null));
        }

        String where = String.join(criteria.combinator() == Combinator.OR ? " OR " : " AND ", terms);
        return new AtomicUpdateSpec(where, set, List.of(), List.of(), false, countForm, ReturnDocument.AFTER, "", paramMapSpec);
    }

    private static List<String> parameterNames(String paramMapSpec) {
        List<String> names = new ArrayList<>();

        if (paramMapSpec != null && !paramMapSpec.isEmpty()) {
            for (String entry : paramMapSpec.split(",")) {
                String[] nameAndIndex = entry.split(":");
                int index = Integer.parseInt(nameAndIndex[1]);

                while (names.size() <= index) {
                    names.add(null);
                }

                names.set(index, nameAndIndex[0]);
            }
        }

        return names;
    }

    private static String term(String methodName, Condition c, List<String> names) {
        if (c.ignoreCase()) {
            throw new IllegalStateException(methodName + ": IgnoreCase is not supported in an updateBy method; use @AtomicUpdate or @Query(\"UPDATE ...\")");
        }

        String f = c.field();

        return switch (c.operator()) {
            case EQ -> f + " = " + param(methodName, c.paramIndex(), names);
            case NE -> f + " <> " + param(methodName, c.paramIndex(), names);
            case GT -> f + " > " + param(methodName, c.paramIndex(), names);
            case GTE -> f + " >= " + param(methodName, c.paramIndex(), names);
            case LT -> f + " < " + param(methodName, c.paramIndex(), names);
            case LTE -> f + " <= " + param(methodName, c.paramIndex(), names);
            case BETWEEN -> f + " BETWEEN " + param(methodName, c.paramIndex(), names) + " AND " + param(methodName, c.paramIndex2(), names);
            case IN -> f + " IN " + param(methodName, c.paramIndex(), names);
            case NIN -> f + " NOT IN " + param(methodName, c.paramIndex(), names);
            case LIKE -> f + " LIKE " + param(methodName, c.paramIndex(), names);
            case IS_NULL -> f + " IS NULL";
            case IS_NOT_NULL -> f + " IS NOT NULL";
            case IS_TRUE -> f + " = TRUE";
            case IS_FALSE -> f + " = FALSE";
            default -> throw new IllegalStateException(methodName + ": the condition " + c.operator() + " on " + f + " is not supported in an updateBy method (equality, comparison, Between, In, NotIn, "
                    + "Like, Null, NotNull, True and False are); use @AtomicUpdate or @Query(\"UPDATE ...\")");
        };
    }

    private static String param(String methodName, int index, List<String> names) {
        if (index < 0 || index >= names.size() || names.get(index) == null) {
            throw new IllegalStateException(methodName + ": parameter " + (index + 1) + " is named by the condition but the method has no (named) parameter there; compile with -parameters or name it with @Param");
        }

        return ":" + names.get(index);
    }
}
