package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public interface MorphiumMongoDriver {

    void setHostSeed(String... host);

    void connect();

    boolean isConnected();

    void close();

    Map<String, Object> getStats();

    Map<String, Object> getOps(long threshold);

    MorphiumDb getDB(String name);

    Map<String, Object> runCommand(Map<String, Object> cmd);

    Map<String, Object> find(Map<String, Object> findCommand);
}
