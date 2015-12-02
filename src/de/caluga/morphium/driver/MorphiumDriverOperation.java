package de.caluga.morphium.driver;

import java.util.Map;

/**
 * Created by stephan on 09.11.15.
 */
public interface MorphiumDriverOperation {

    public Map<String, Object> execute() throws MorphiumDriverNetworkException;

    public String toString();
}
