package de.caluga.morphium;

/**
 * Created by stephan on 17.07.15.
 */
public interface LoggerDelegate {
    void log(String loggerName, int lv, String msg, Throwable t);
}
