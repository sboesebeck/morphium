package de.caluga.morphium.driver;

/**
 * A request reached the wire, but no reply for it ever arrived: the peer closed the connection
 * (a clean FIN at a message boundary - an idle connection the server dropped from its own pool,
 * a restart, a deliberately unanswered command) or the reply did not make it back within the
 * caller's budget.
 *
 * <p>Deliberately a {@link MorphiumDriverNetworkException}: the request provably left this
 * client on a connection that can no longer serve it, so every caller may retry it on a fresh
 * connection exactly like any other network error. Before this existed the read path returned
 * {@code null} instead, which the command wrappers turned into NPEs ({@code res.get(...)} on a
 * null answer, e.g. {@code listDatabases}, {@code replSetGetStatus}, {@code distinct}) or - worse
 * for reads - into a silently empty result set.
 */
public class MorphiumDriverNoReplyException extends MorphiumDriverNetworkException {
    public MorphiumDriverNoReplyException(String message) {
        super(message);
    }

    public MorphiumDriverNoReplyException(String message, Throwable cause) {
        super(message, cause);
    }
}
