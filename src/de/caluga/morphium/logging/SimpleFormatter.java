package de.caluga.morphium.logging;

/**
 * User: stephan
 * Date: 25.02.12
 * Time: 17:41
 * Simple formatter to be used with util.logging. Ist prints data in one line - best for console
 */

import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class SimpleFormatter extends Formatter {
    private final Date date = new Date();
    private final DateFormat dateformater = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

    /**
     * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
     */
    @Override

    public String format(LogRecord record) {
        date.setTime(record.getMillis());

        StringBuilder sb = new StringBuilder();

        // Date
        sb.append(dateformater.format(date));
        sb.append(" - ");

        // Class
        if (record.getSourceClassName() != null) {
            sb.append(record.getSourceClassName());
        } else {
            sb.append(record.getLoggerName());
        }

        // Method
        if (record.getSourceMethodName() != null) {
            sb.append("@");
            sb.append(record.getSourceMethodName());
            sb.append("() ");
        }

        // Level
        sb.append(": ");
        sb.append(record.getLevel().getName());
        sb.append(" - ");

        // Message
        sb.append(record.getMessage());

        // Newline
        sb.append(System.getProperty("line.separator"));

        return sb.toString();
    }
}
