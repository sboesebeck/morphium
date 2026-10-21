package de.caluga.morphium.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The entity that a JDQL statement names (MOR-077): {@code DELETE FROM Box}, {@code UPDATE Box SET ...}, {@code SELECT ... FROM Box}, {@code FROM Box WHERE ...}. A statement that names none
 * ({@code WHERE id = ?1}) works on the entity of the method.
 */
final class JdqlEntityName {

    private static final Pattern FROM = Pattern.compile("(?is)(?:^|\\s)FROM\\s+([A-Za-z_][A-Za-z0-9_.]*)");

    private JdqlEntityName() {
    }

    /** the name of the entity, null if the statement names none */
    static String of(String jdql) {
        if (jdql == null || jdql.isBlank()) {
            return null;
        }

        if (JdqlDelete.is(jdql)) {
            return JdqlDelete.entityName(jdql);
        }

        if (JdqlUpdate.is(jdql)) {
            return JdqlUpdate.parse(jdql).entityName();
        }

        Matcher m = FROM.matcher(JdqlLiterals.maskStrings(jdql));

        if (!m.find()) {
            return null;
        }

        String name = jdql.substring(m.start(1), m.end(1));
        return name.equalsIgnoreCase("WHERE") ? null : name;
    }
}
