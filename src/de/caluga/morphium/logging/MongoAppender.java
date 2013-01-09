package de.caluga.morphium.logging;

import de.caluga.morphium.MorphiumSingleton;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import java.util.Arrays;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.12
 * Time: 16:47
 * <p/>
 */
public class MongoAppender implements Appender {
    private Filter filter;
    private ErrorHandler errorHandler;
    private Layout layout;
    private String name;

    @Override
    public void addFilter(Filter newFilter) {
        filter = newFilter;
    }

    @Override
    public Filter getFilter() {
        return filter;
    }

    @Override
    public void clearFilters() {
        filter = null;
    }

    @Override
    public void close() {

    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void doAppend(LoggingEvent event) {
        if (!MorphiumSingleton.isConfigured() || !MorphiumSingleton.isInitialized()) return;
        Log l = new Log();
        if (event.getMessage() != null)
            l.setMessage(event.getMessage().toString());
        if (event.getRenderedMessage() != null)
            l.setMessage(event.getRenderedMessage());
        l.setTimestamp(event.getTimeStamp());
        l.setLevel(event.getLevel().toString());
        l.setThreadName(event.getThreadName());
        l.setThreadId((int) Thread.currentThread().getId());
        if (event.getThrowableInformation() != null) {
            ThrowableInformation thr = event.getThrowableInformation();
            Throwable t = thr.getThrowable();
            l.setExceptionMessage(t.getMessage());
            l.setExceptionName(t.getClass().getName());
            l.setCausedBy(t.getCause().getClass().getName() + "(" + t.getCause().getMessage() + ")");
            l.setExceptionStacktrace(Arrays.asList(thr.getThrowableStrRep()));
        }
        MorphiumSingleton.get().store(l);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public void setLayout(Layout layout) {
        this.layout = layout;
    }

    @Override
    public Layout getLayout() {
        return layout;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
