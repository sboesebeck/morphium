package de.caluga.morphium.data;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The literals of a JDQL query (MOR-082): string literals in single quotes with {@code ''} for a quote inside, lists such as {@code ('a', 'b', :p)} after {@code IN}, and
 * the qualified constant of an enum ({@code com.example.Kind.PRIME}). {@link JdqlParser} keeps an operand as written; this class tells what is inside.
 */
final class JdqlLiterals {

    private static final char MASK = '\u0001';
    private static final Pattern QUALIFIED_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)+");

    private JdqlLiterals() {
    }

    /**
     * The text with the content of every string literal replaced by one placeholder character each, the quotes stay. The result has the length of the input, so an
     * index that is found in it is an index in the input: this is how keywords, operators, commas and parentheses are searched without hitting one inside a literal.
     */
    static String maskStrings(String text) {
        StringBuilder out = new StringBuilder(text.length());
        boolean inside = false;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);

            if (c == '\'') {
                inside = !inside;
                out.append(c);
            } else {
                out.append(inside ? MASK : c);
            }
        }

        return out.toString();
    }

    /** the content of a string literal with {@code ''} turned into {@code '} */
    static String unquote(String literal) {
        return literal.substring(1, literal.length() - 1).replace("''", "'");
    }

    static boolean isStringLiteral(String operand) {
        return operand.length() >= 2 && operand.charAt(0) == '\'' && operand.charAt(operand.length() - 1) == '\'';
    }

    /** whether the operand is {@code (item, item, ...)} */
    static boolean isList(String operand) {
        return operand.length() >= 2 && operand.charAt(0) == '(' && operand.charAt(operand.length() - 1) == ')';
    }

    /** the items of {@code (a, 'b,c', :p)} as written, split at the commas outside of string literals */
    static List<String> splitList(String list) {
        String inner = list.substring(1, list.length() - 1);
        String masked = maskStrings(inner);
        List<String> items = new ArrayList<>();
        int start = 0;

        for (int i = 0; i <= masked.length(); i++) {
            if (i == masked.length() || masked.charAt(i) == ',') {
                String item = inner.substring(start, i).trim();

                if (!item.isEmpty()) {
                    items.add(item);
                }

                start = i + 1;
            }
        }

        return items;
    }

    /** whether the operand is a name with dots, the way an enum constant is written in a query: {@code com.example.Kind.PRIME} */
    static boolean isQualifiedName(String operand) {
        return QUALIFIED_NAME.matcher(operand).matches();
    }

    /** the constant of {@code com.example.Kind.PRIME}: Morphium stores an enum by its name */
    static String constantName(String qualifiedName) {
        return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
    }
}
