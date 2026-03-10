package de.caluga.morphium.driver;

/**
 * Created by stephan on 09.11.15.
 */
@SuppressWarnings("DefaultFileTemplate")
public interface MorphiumDriverOperation<V> {

    V execute() throws MorphiumDriverException;

    String toString();
}
