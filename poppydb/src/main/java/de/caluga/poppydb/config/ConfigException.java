package de.caluga.poppydb.config;

/**
 * Signals a fatal problem while discovering, reading or validating a PoppyDB configuration
 * file (e.g. an unreadable explicit --cfg path, an unknown key, a value that does not parse,
 * or a secrets file with unsafe permissions).
 * <p>
 * {@link ConfigLoader} never calls {@code System.exit(...)} itself so its behaviour stays
 * testable - {@link de.caluga.poppydb.PoppyDBCLI#main(String[])} catches this exception,
 * logs the message and exits the process.
 */
public class ConfigException extends RuntimeException {
    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
