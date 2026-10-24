package de.caluga.morphium.data;

import java.util.Locale;

/**
 * The JDQL statement {@code DELETE FROM Entity [WHERE ...]} of a {@code @Query} method (MOR-017). {@link JdqlParser} reads queries; a delete
 * statement is split here into its entity name and the {@code WHERE} clause, which the parser then reads like the one of a query.
 */
public final class JdqlDelete {

    private JdqlDelete() {
    }

    /** whether the statement is a {@code DELETE} statement */
    public static boolean is(String jdql) {
        if (jdql == null) {
            return false;
        }

        String t = jdql.stripLeading();
        return t.length() > 6 && t.regionMatches(true, 0, "DELETE", 0, 6) && Character.isWhitespace(t.charAt(6));
    }

    /**
     * The entity name of {@code DELETE FROM Entity ...}, or null if the statement names none ({@code DELETE FROM WHERE ...}, {@code DELETE FROM}).
     *
     * @throws IllegalArgumentException if the statement is not {@code DELETE FROM ...}
     */
    public static String entityName(String jdql) {
        String afterFrom = afterFrom(jdql);
        String first = firstToken(afterFrom);
        return first.isEmpty() || first.equalsIgnoreCase("WHERE") ? null : first;
    }

    /**
     * The {@code WHERE ...} clause of the statement, empty if there is none.
     *
     * @throws IllegalArgumentException if the statement is not {@code DELETE FROM ...}
     */
    public static String whereClause(String jdql) {
        String afterFrom = afterFrom(jdql);
        String first = firstToken(afterFrom);

        if (first.isEmpty()) {
            return "";
        }

        String rest = first.equalsIgnoreCase("WHERE") ? afterFrom : afterFrom.substring(first.length()).stripLeading();
        return rest.stripTrailing();
    }

    private static String afterFrom(String jdql) {
        if (!is(jdql)) {
            throw new IllegalArgumentException("Not a DELETE statement: \"" + jdql + "\"");
        }

        String t = jdql.strip().substring(6).stripLeading();

        if (!(t.length() >= 4 && t.regionMatches(true, 0, "FROM", 0, 4) && (t.length() == 4 || Character.isWhitespace(t.charAt(4))))) {
            throw new IllegalArgumentException("A DELETE statement is \"DELETE FROM Entity [WHERE ...]\": \"" + jdql + "\"");
        }

        return t.substring(4).stripLeading();
    }

    private static String firstToken(String s) {
        int end = 0;

        while (end < s.length() && !Character.isWhitespace(s.charAt(end))) {
            end++;
        }

        return s.substring(0, end);
    }

    /** whether the clause starts with a keyword that a DELETE statement does not allow ({@code ORDER BY}, {@code GROUP BY}, {@code HAVING}) */
    static boolean hasForbiddenClause(String where) {
        String upper = where.toUpperCase(Locale.ROOT);
        return upper.contains(" ORDER BY ") || upper.startsWith("ORDER BY ") || upper.contains(" GROUP BY ") || upper.contains(" HAVING ");
    }
}
