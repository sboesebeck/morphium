package de.caluga.poppydb.config;

import java.util.List;

/**
 * One entry of a users file, already validated by {@link UsersFileLoader}: {@code user} and
 * {@code pwd} are guaranteed non-empty, {@code db} defaults to {@code "admin"} when the file
 * omits it, {@code roles} is carried through opaquely (mongod-shaped, not enforced anywhere in
 * PoppyDB) and {@code mechanisms} is an optional list of mechanism names. Dumb data holder - no
 * behavior beyond {@link #toString()} redacting {@code pwd}.
 * <p>
 * {@link #toString()} deliberately never renders {@code pwd} - this is the last line of defense
 * against the secret leaking into a log line via {@code "{}"}-style formatting somewhere
 * downstream (bootstrap apply, debug logging, ...).
 */
public record UserSpec(String user, String db, String pwd, List<Object> roles, List<String> mechanisms) {

    @Override
    public String toString() {
        return "UserSpec{user='" + user + "', db='" + db + "', pwd='***', roles=" + roles
                + ", mechanisms=" + mechanisms + "}";
    }
}
