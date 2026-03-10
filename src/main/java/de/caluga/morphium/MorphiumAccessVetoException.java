package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 10:09
 * <p>
 * This exception should be raised when the access should not be allowed
 */
public class MorphiumAccessVetoException extends RuntimeException {
    public MorphiumAccessVetoException() {
        super();
    }

    public MorphiumAccessVetoException(String msg) {
        super(msg);
    }
}
