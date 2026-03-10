package de.caluga.morphium.replicaset;

import java.util.Map;

/**
 * Created by stephan on 15.11.16.
 */
public interface OplogListener {
    void incomingData(Map<String, Object> data);
}
