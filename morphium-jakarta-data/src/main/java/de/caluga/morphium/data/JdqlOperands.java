package de.caluga.morphium.data;

import java.util.regex.Pattern;

/**
 * The operand check of {@link JdqlParser}: every operand of a condition is a parameter ({@code :name} or {@code ?1}) or a literal. Before MOR-075 the parser took whatever
 * stood after the operator as the operand, so {@code id == :id} became the condition {@code id = "= :id"}: it matched nothing and nobody told. The parser refuses it now; this class
 * keeps the rule ({@link #isOperand}) and gives the statements that write ({@code DELETE}, {@code UPDATE}, {@code @AtomicUpdate}) a way to report it at build time.
 */
public final class JdqlOperands {

    private static final Pattern OPERAND = Pattern.compile(":[A-Za-z_][A-Za-z0-9_]*|\\?\\d+|'([^']|'')*'|\"[^\"]*\"|(?i:true|false|null)");

    /** the text of the parse error for an operand that is neither a parameter nor a literal, see {@link JdqlParser} */
    static final String OPERAND_PROBLEM = "is neither a parameter";

    private JdqlOperands() {
    }

    /** a parameter or a literal, a list of them after IN, or the qualified constant of an enum */
    static boolean isOperand(String ref) {
        if (JdqlLiterals.isList(ref)) {
            return JdqlLiterals.splitList(ref).stream().allMatch(JdqlOperands::isOperand);
        }

        return OPERAND.matcher(ref).matches() || JdqlLiterals.isNumber(ref) || JdqlLiterals.isQualifiedName(ref);
    }

    /**
     * @param whereClause a {@code WHERE ...} clause (or the condition without the keyword)
     * @return null if the clause is fine, otherwise the message of the parser, which names the operand and the position
     * @throws IllegalArgumentException for a clause that the parser cannot split for another reason
     */
    public static String check(String whereClause) {
        if (whereClause == null || whereClause.isBlank()) {
            return null;
        }

        String clause = whereClause.strip();

        try {
            JdqlParser.parse(clause.regionMatches(true, 0, "WHERE ", 0, 6) ? clause : "WHERE " + clause);
            return null;
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().contains(OPERAND_PROBLEM)) {
                return e.getMessage();
            }

            throw e;
        }
    }
}
