package de.caluga.morphium.driver;

/**
 * a driver operation
 *
 * @param <V> the return type of the operation
 */
@SuppressWarnings("DefaultFileTemplate")
public interface MorphiumDriverOperation<V> {

    V execute() throws MorphiumDriverException;

    String toString();
}
