package de.caluga.morphium.driver;

/**
 * The server applied a write but could not satisfy the requested write concern in time (the {@code writeConcernError} of a write reply, GitHub #408). The data
 * IS written: do not retry a non-idempotent update ({@code $inc}, {@code $push}) because of this exception. {@link #getMongoCode()} is the code of the
 * write concern error.
 */
public class WriteConcernNotSatisfiedException extends MorphiumDriverException {
    public WriteConcernNotSatisfiedException(String message) {
        super(message);
    }
}
