package de.caluga.morphium.driver;

import java.util.Map;

/**
 * Created by stephan on 29.07.16.
 */
public interface DriverTailableIterationCallback {
    /**
     * @param data - incoming data
     * @param dur  - duration since start
     * @return true, if you want to continue reading
     */
    void incomingData(Doc data, long dur);

    boolean isContinued();
}
