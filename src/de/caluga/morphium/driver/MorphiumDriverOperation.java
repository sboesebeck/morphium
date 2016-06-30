package de.caluga.morphium.driver;

import java.util.Map;

/**
 * Created by stephan on 09.11.15.
 */
@SuppressWarnings("DefaultFileTemplate")
public interface MorphiumDriverOperation {

    Map<String, Object> execute() throws MorphiumDriverException;

    String toString();
}
