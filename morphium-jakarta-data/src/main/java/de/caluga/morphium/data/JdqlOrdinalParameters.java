package de.caluga.morphium.data;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Ordinal parameters ({@code ?1}, {@code ?2}, ...) of a JDQL query. The parser knows named parameters only, so a query with ordinals is
 * rewritten to names ({@code ?1} becomes {@code :_p1}) and the names are bound to the method arguments: ordinal <i>n</i> is the <i>n</i>-th
 * argument that is not a special parameter ({@code Sort}, {@code Order}, {@code PageRequest}, {@code Limit}), counting from one. Question marks
 * inside a string literal are not parameters. A query uses either ordinal or named parameters, never both.
 */
public final class JdqlOrdinalParameters {

    private static final String PREFIX = "_p";

    private JdqlOrdinalParameters() {
    }

    /** whether the query has at least one ordinal parameter outside of a string literal */
    public static boolean has(String jdql) {
        return !scan(jdql).ordinals.isEmpty();
    }

    /** the highest ordinal that the query uses, 0 if it uses none */
    public static int highest(String jdql) {
        Scan scan = scan(jdql);
        return scan.ordinals.isEmpty() ? 0 : scan.ordinals.last();
    }

    /** whether the query uses ordinal and named parameters together */
    public static boolean mixesStyles(String jdql) {
        Scan scan = scan(jdql);
        return !scan.ordinals.isEmpty() && scan.named;
    }

    /** the query with {@code ?n} replaced by {@code :_pn} */
    public static String rewrite(String jdql) {
        return scan(jdql).rewritten;
    }

    /**
     * The binding for {@link JdqlMethodBridge}: {@code "_p1:<index>,_p2:<index>"} for the ordinals that the query uses.
     *
     * @param argCount the number of arguments of the method; the indexes of the special parameters are -1 if there is none
     * @throws IllegalArgumentException if the query uses an ordinal that is below one or has no argument
     */
    public static String paramMapSpec(String jdql, int argCount, int sortIdx, int orderIdx, int pageRequestIdx, int limitIdx) {
        List<Integer> queryArgs = new ArrayList<>();

        for (int i = 0; i < argCount; i++) {
            if (i != sortIdx && i != orderIdx && i != pageRequestIdx && i != limitIdx) {
                queryArgs.add(i);
            }
        }

        StringBuilder spec = new StringBuilder();

        for (int ordinal : scan(jdql).ordinals) {
            if (ordinal < 1 || ordinal > queryArgs.size()) {
                throw new IllegalArgumentException("Parameter ?" + ordinal + " in the query \"" + jdql + "\" has no argument: the method has "
                    + queryArgs.size() + " query parameter(s), counted without Sort, Order, PageRequest and Limit; ordinals start at 1");
            }

            if (spec.length() > 0) {
                spec.append(',');
            }

            spec.append(PREFIX).append(ordinal).append(':').append(queryArgs.get(ordinal - 1));
        }

        return spec.toString();
    }

    private static final class Scan {
        final TreeSet<Integer> ordinals = new TreeSet<>();
        boolean named;
        String rewritten;
    }

    private static Scan scan(String jdql) {
        Scan scan = new Scan();

        if (jdql == null) {
            scan.rewritten = null;
            return scan;
        }

        StringBuilder out = new StringBuilder(jdql.length() + 8);
        boolean inLiteral = false;

        for (int i = 0; i < jdql.length(); i++) {
            char c = jdql.charAt(i);

            if (c == '\'') {
                inLiteral = !inLiteral;
                out.append(c);
            } else if (!inLiteral && c == '?' && i + 1 < jdql.length() && Character.isDigit(jdql.charAt(i + 1))) {
                int end = i + 1;

                while (end < jdql.length() && Character.isDigit(jdql.charAt(end))) {
                    end++;
                }

                int ordinal = Integer.parseInt(jdql.substring(i + 1, end));
                scan.ordinals.add(ordinal);
                out.append(':').append(PREFIX).append(ordinal);
                i = end - 1;
            } else {
                if (!inLiteral && c == ':' && i + 1 < jdql.length()
                    && (Character.isLetter(jdql.charAt(i + 1)) || jdql.charAt(i + 1) == '_')) {
                    scan.named = true;
                }

                out.append(c);
            }
        }

        scan.rewritten = out.toString();
        return scan;
    }
}
