package de.caluga.morphium;

import java.util.Map;

/**
 * Created by stephan on 18.09.15.
 */
@SuppressWarnings("DefaultFileTemplate")
public interface TypeMapper<T> {

    Map<String, Object> marshall(T o);

    T unmarshall(Map<String, Object> d);

    boolean matches(Map<String, Object> value);
}
