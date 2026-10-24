package de.caluga.morphium.data;

import java.util.regex.Pattern;

/**
 * A check that {@link JdqlParser} does not make: every operand of a condition is a parameter ({@code :name} or {@code ?1}) or a literal. The parser takes whatever stands after the
 * operator as the operand, so {@code id == :id} becomes the condition {@code id = "= :id"}: it matches nothing and nobody tells. For the statements that write
 * ({@code DELETE}, {@code UPDATE}, {@code @AtomicUpdate}) such a typo should fail at build time or when the repository is created, not run.
 */
public final class JdqlOperands {

    private static final Pattern OPERAND = Pattern.compile(":[A-Za-z_][A-Za-z0-9_]*|\\?\\d+|[+-]?\\d+(\\.\\d+)?|'([^']|'')*'|\"[^\"]*\"|(?i:true|false|null)");

    private JdqlOperands() {
    }

    /**
     * @param whereClause a {@code WHERE ...} clause (or the condition without the keyword)
     * @return null if the clause is fine, otherwise a message that names the operand
     */
    public static String check(String whereClause) {
        if (whereClause == null || whereClause.isBlank()) {
            return null;
        }

        String clause = whereClause.strip();
        JdqlQuery query = JdqlParser.parse(clause.regionMatches(true, 0, "WHERE ", 0, 6) ? clause : "WHERE " + clause);
        return check(query.conditions());
    }

    private static String check(java.util.List<JdqlQuery.JdqlCondition> conditions) {
        if (conditions == null) {
            return null;
        }

        for (JdqlQuery.JdqlCondition c : conditions) {
            for (String ref : new String[] {c.valueRef(), c.valueRef2()}) {
                if (ref != null && !OPERAND.matcher(ref.strip()).matches()) {
                    return "the operand \"" + ref + "\" of the condition on " + c.fieldName() + " is neither a parameter (:name) nor a literal";
                }
            }

            String nested = check(c.groupConditions());

            if (nested != null) {
                return nested;
            }
        }

        return null;
    }
}
