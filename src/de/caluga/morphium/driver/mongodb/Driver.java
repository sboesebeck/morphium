package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 05.11.15.
 */

import de.caluga.morphium.driver.MorphiumDb;
import de.caluga.morphium.driver.MorphiumMongoDriver;

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class Driver implements MorphiumMongoDriver {
    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public void connect() {

    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getStats() {
        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) {
        return null;
    }

    @Override
    public MorphiumDb getDB(String name) {
        return null;
    }
}
