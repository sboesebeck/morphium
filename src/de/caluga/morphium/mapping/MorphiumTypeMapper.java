package de.caluga.morphium.mapping;

/**
 * User: Stephan Bösebeck
 * Date: 02.08.18
 * Time: 22:25
 * <p>
 * TODO: Add documentation here
 */
public interface MorphiumTypeMapper<T> {
    Object marshall(T o);

    T unmarshall(Object d);
}
