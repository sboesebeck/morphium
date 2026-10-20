package de.caluga.morphium.driver;

import java.util.Map;

/**
 * The verdict of a command reply's {@code ok} field - the one place that knows how servers
 * encode it, so call sites stop comparing it literally.
 *
 * <p>MongoDB (and the server-compatible backends Morphium talks to) writes {@code ok} as a
 * double on most paths ({@code 1.0} / {@code 0.0}) but as int32 on others, and PoppyDB's own
 * error paths do the same ({@code Doc.of("ok", 0, ...)}). A literal {@code reply.get("ok")
 * .equals(0.0)} therefore reads an integer {@code 0} as <em>success</em> - silently swallowing
 * the command error - and a literal {@code .equals(1.0)} reads an integer {@code 1} as
 * <em>failure</em>, inventing an error ("Error: null - null") for a perfectly good reply. Both
 * bugs existed in this codebase at once, in different classes.
 *
 * <p>Three outcomes, deliberately - the two ambiguous ones are not collapsed:
 * <ul>
 *     <li>{@link #OK} - a numeric {@code ok} of 1: the command succeeded.</li>
 *     <li>{@link #FAILURE} - a numeric {@code ok} of 0: the command failed, the reply names
 *     {@code code}/{@code errmsg}.</li>
 *     <li>{@link #UNKNOWN} - no {@code ok} field, or one that is not a number (or a number that
 *     is neither 1 nor 0). Only the call site can decide what to do with a reply whose status it
 *     cannot judge: the read path lets the payload through (the data is there, and a throw would
 *     break a working read), the authentication handshake treats it as a failure.</li>
 * </ul>
 */
public enum CommandStatus {
    OK, FAILURE, UNKNOWN;

    /** Null-safe: a missing reply, or one without an {@code ok} field, is {@link #UNKNOWN}. */
    public static CommandStatus of(Map<String, ?> reply) {
        return reply == null ? UNKNOWN : of(reply.get("ok"));
    }

    public static CommandStatus of(Object okValue) {
        if (okValue instanceof Number number) {
            double value = number.doubleValue();

            if (value == 1.0) {
                return OK;
            }

            if (value == 0.0) {
                return FAILURE;
            }
        }

        return UNKNOWN;
    }

    public boolean isFailure() {
        return this == FAILURE;
    }

    public boolean isOk() {
        return this == OK;
    }
}
