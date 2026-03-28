package de.caluga.morphium.objectmapping;

/**
 * User: Stephan Bösebeck
 * Date: 02.08.18
 * Time: 22:25
 * <p>
 * interface for custom type mappers
 * </p>
 *
 * @param <T> the type to map
 */
public interface MorphiumTypeMapper<T> {
    Object marshall(T o);

    T unmarshall(Object d);
}
