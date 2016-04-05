package de.caluga.morphium;/**
 * Created by stephan on 14.08.15.
 */

import java.util.logging.Level;

/**
 * logger delegate that passes on all calls to JUL
 **/
public class JavaUtilLoggingDelegate implements LoggerDelegate {
    @Override
    public void log(String loggerName, int lv, String msg, Throwable t) {
        java.util.logging.Level level;

        switch (lv) {
            case 0:
                return;
            case 1:
            case 2:
                level = Level.SEVERE;
                break;
            case 3:
                level = Level.WARNING;
                break;
            case 4:
                level = Level.INFO;
                break;
            case 5:
                level = Level.ALL;
                break;
            default:
                level = Level.ALL;
                break;
        }
        java.util.logging.Logger.getLogger(loggerName).log(level, msg, t);
    }
}
