package de.caluga.morphium;/**
 * Created by stephan on 14.08.15.
 */

import java.util.logging.Level;

/**
 * logger delegate that passes on all calls to JUL
 **/
public class JavaUtilLoggingDelegate implements LoggerDelegate {
    @SuppressWarnings("WeakerAccess")
    public final static int LOG_OFF = 0;
    @SuppressWarnings("WeakerAccess")
    public final static int LOG_SEVERE = 1;
    @SuppressWarnings("WeakerAccess")
    public final static int LOG_SEVERER = 2;
    @SuppressWarnings("WeakerAccess")
    public final static int LOG_WARN = 3;
    @SuppressWarnings("WeakerAccess")
    public final static int LOG_INFO = 4;
    @SuppressWarnings("WeakerAccess")
    public final static int LOG_ALL = 5;

    @Override
    public void log(String loggerName, int lv, String msg, Throwable t) {
        java.util.logging.Level level;

        switch (lv) {
            case LOG_OFF:
                return;
            case LOG_SEVERE:
            case LOG_SEVERER:
                level = Level.SEVERE;
                break;
            case LOG_WARN:
                level = Level.WARNING;
                break;
            case LOG_INFO:
                level = Level.INFO;
                break;
            case LOG_ALL:
            default:
                level = Level.ALL;
                break;
        }
        java.util.logging.Logger.getLogger(loggerName).log(level, msg, t);
    }
}
