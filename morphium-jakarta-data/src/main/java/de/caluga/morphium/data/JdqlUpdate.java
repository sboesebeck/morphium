package de.caluga.morphium.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The JDQL statement {@code UPDATE Entity SET item [, item ...] [WHERE ...]} of a {@code @Query} method (MOR-018). The statement is split here
 * into the entity name, the {@code SET} items and the {@code WHERE} clause, which {@link JdqlParser} reads like the one of a query. It is executed
 * as ONE update command by {@link JdqlMethodBridge#executeJdqlUpdate}, so the items must map to the classic update operators:
 * <table>
 * <caption>SET items</caption>
 * <tr><td>{@code f = :x}, {@code f = 5}, {@code f = 'text'}, {@code f = TRUE}</td><td>{@code $set}</td></tr>
 * <tr><td>{@code f = NULL}</td><td>{@code $unset} (a query for {@code f IS NULL} finds it, the field is not stored)</td></tr>
 * <tr><td>{@code f = f + :n}, {@code f = f - :n}</td><td>{@code $inc} (with the negated operand for minus)</td></tr>
 * <tr><td>{@code f = f * :n}</td><td>{@code $mul}</td></tr>
 * </table>
 * Everything else, among it division, a field on the right side that is not the field itself, {@code ||} and functions, would need an aggregation
 * pipeline update and is refused with a message that names the item.
 */
public final class JdqlUpdate {

    /** the kind of a SET item */
    public enum Op { SET, UNSET, INC, DEC, MUL }

    /**
     * One item of the SET clause.
     *
     * @param value for SET, INC, DEC and MUL a parameter reference ({@code :name}) or a literal (number, quoted string, TRUE, FALSE); null for UNSET
     */
    public record Item(String field, Op op, String value) {
    }

    /**
     * @param entityName the entity of the statement, null if it names none
     * @param where      the {@code WHERE ...} clause, empty if there is none
     */
    public record Statement(String entityName, List<Item> items, String where) {
    }

    private static final Pattern FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");
    // a named (:name) or an ordinal (?1) parameter; the build and the proxy check the statement as written, the bridge rewrites ordinals first
    private static final Pattern PARAM = Pattern.compile(":[A-Za-z_][A-Za-z0-9_]*|\\?\\d+");
    private static final Pattern NUMBER = Pattern.compile("[+-]?\\d+(\\.\\d+)?");
    private static final Pattern STRING = Pattern.compile("'([^']|'')*'");
    private static final Pattern ARITHMETIC = Pattern.compile("([A-Za-z_][A-Za-z0-9_.]*)\\s*([-+*/])\\s*(.+)");

    private JdqlUpdate() {
    }

    /** whether the statement is an {@code UPDATE} statement */
    public static boolean is(String jdql) {
        if (jdql == null) {
            return false;
        }

        String t = jdql.stripLeading();
        return t.length() > 6 && t.regionMatches(true, 0, "UPDATE", 0, 6) && Character.isWhitespace(t.charAt(6));
    }

    /**
     * Splits the statement and checks every SET item.
     *
     * @throws IllegalArgumentException if the statement is not valid or an item cannot be one update command; the message names the item
     */
    public static Statement parse(String jdql) {
        if (!is(jdql)) {
            throw new IllegalArgumentException("Not an UPDATE statement: \"" + jdql + "\"");
        }

        String rest = jdql.strip().substring(6).stripLeading();
        int space = indexOfWhitespace(rest);
        String first = space < 0 ? rest : rest.substring(0, space);
        String entityName = null;

        if (!first.equalsIgnoreCase("SET")) {
            entityName = first;
            rest = space < 0 ? "" : rest.substring(space).stripLeading();
            space = indexOfWhitespace(rest);
            String second = space < 0 ? rest : rest.substring(0, space);

            if (!second.equalsIgnoreCase("SET")) {
                throw new IllegalArgumentException("An UPDATE statement is \"UPDATE Entity SET field = value, ... [WHERE ...]\": \"" + jdql + "\"");
            }
        }

        space = indexOfWhitespace(rest);
        String afterSet = space < 0 ? "" : rest.substring(space).strip();
        int whereAt = indexOfKeyword(afterSet, "WHERE");
        String setPart = (whereAt < 0 ? afterSet : afterSet.substring(0, whereAt)).strip();
        String where = whereAt < 0 ? "" : afterSet.substring(whereAt).strip();

        if (setPart.isEmpty()) {
            throw new IllegalArgumentException("The UPDATE statement has no SET items: \"" + jdql + "\"");
        }

        List<Item> items = new ArrayList<>();
        List<String> fields = new ArrayList<>();

        for (String raw : splitOutsideQuotes(setPart, ',')) {
            Item item = parseItem(raw.strip(), jdql);

            if (fields.contains(item.field())) {
                throw new IllegalArgumentException("The field " + item.field() + " is set twice in \"" + jdql + "\"");
            }

            fields.add(item.field());
            items.add(item);
        }

        return new Statement(entityName, List.copyOf(items), where);
    }

    private static Item parseItem(String raw, String jdql) {
        int eq = raw.indexOf('=');

        if (eq <= 0) {
            throw new IllegalArgumentException("SET item \"" + raw + "\" is not \"field = value\": \"" + jdql + "\"");
        }

        String field = raw.substring(0, eq).strip();
        String rhs = raw.substring(eq + 1).strip();

        if (!FIELD.matcher(field).matches()) {
            throw new IllegalArgumentException("SET item \"" + raw + "\": \"" + field + "\" is not a field name");
        }

        if (rhs.equalsIgnoreCase("NULL")) {
            return new Item(field, Op.UNSET, null);
        }

        if (isOperand(rhs)) {
            return new Item(field, Op.SET, rhs);
        }

        Matcher m = ARITHMETIC.matcher(rhs);

        if (m.matches()) {
            String left = m.group(1);
            String operator = m.group(2);
            String operand = m.group(3).strip();

            if (!left.equals(field)) {
                throw new IllegalArgumentException("SET item \"" + raw + "\" is not supported: the value of one field cannot be computed from another field in a single update command");
            }

            if (operator.equals("/")) {
                throw new IllegalArgumentException("SET item \"" + raw + "\" is not supported: division has no update operator, multiply by the inverse instead");
            }

            if (!PARAM.matcher(operand).matches() && !NUMBER.matcher(operand).matches()) {
                throw new IllegalArgumentException("SET item \"" + raw + "\" is not supported: the operand of " + operator + " has to be a parameter or a number");
            }

            return new Item(field, operator.equals("+") ? Op.INC : operator.equals("-") ? Op.DEC : Op.MUL, operand);
        }

        throw new IllegalArgumentException("SET item \"" + raw + "\" is not supported: only a value, NULL, " + field + " + n, " + field + " - n and " + field + " * n can be one update command");
    }

    private static boolean isOperand(String s) {
        return PARAM.matcher(s).matches() || NUMBER.matcher(s).matches() || STRING.matcher(s).matches() || s.equalsIgnoreCase("TRUE") || s.equalsIgnoreCase("FALSE");
    }

    /** the literal of a value token: a Number, a String, a Boolean, or null if the token is a parameter reference */
    public static Object literal(String token) {
        if (PARAM.matcher(token).matches()) {
            return null;
        }

        if (NUMBER.matcher(token).matches()) {
            if (token.contains(".")) {
                return Double.parseDouble(token);
            }

            long l = Long.parseLong(token.startsWith("+") ? token.substring(1) : token);
            return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? (Object) (int) l : (Object) l;
        }

        if (token.equalsIgnoreCase("TRUE")) {
            return Boolean.TRUE;
        }

        if (token.equalsIgnoreCase("FALSE")) {
            return Boolean.FALSE;
        }

        return token.substring(1, token.length() - 1).replace("''", "'");
    }

    private static int indexOfWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }

        return -1;
    }

    /** index of the keyword outside of string literals, delimited by whitespace; -1 if there is none */
    private static int indexOfKeyword(String s, String keyword) {
        boolean inLiteral = false;
        String upper = s.toUpperCase(Locale.ROOT);

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '\'') {
                inLiteral = !inLiteral;
            } else if (!inLiteral && upper.startsWith(keyword, i) && (i == 0 || Character.isWhitespace(s.charAt(i - 1)))
                       && (i + keyword.length() == s.length() || Character.isWhitespace(s.charAt(i + keyword.length())))) {
                return i;
            }
        }

        return -1;
    }

    private static List<String> splitOutsideQuotes(String s, char separator) {
        List<String> parts = new ArrayList<>();
        boolean inLiteral = false;
        int start = 0;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '\'') {
                inLiteral = !inLiteral;
            } else if (!inLiteral && c == separator) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }

        parts.add(s.substring(start));
        return parts;
    }
}
