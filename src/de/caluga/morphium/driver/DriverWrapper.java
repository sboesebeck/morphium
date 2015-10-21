package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

/**
 * TODO: Add Documentation here
 **/
public interface DriverWrapper {

    void setHostSeed(String... host);

    void connect();

    boolean isConnected();

    void close();

    MongoDocument getStats();

    MongoDocument getOps(long threshold);

    MorphiumDb getDB(String name);
}
