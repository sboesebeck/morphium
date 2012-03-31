/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.logging;

import de.caluga.morphium.Morphium;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * @author stephan
 */
public class MongoHandler extends Handler {

    @Override
    public void publish(LogRecord lr) {
        if (!Morphium.isConfigured()) return;
        Log l = new Log();
        l.setMessage(lr.getMessage());
        if (lr.getThrown() != null) {
            l.setExceptionMessage(lr.getThrown().getMessage());
            l.setExceptionName(lr.getThrown().getClass().getName());
            if (lr.getThrown().getCause() != null) {
                l.setCausedBy(lr.getThrown().getCause().getClass().getName() + " @" + lr.getThrown().getCause().getStackTrace()[0].getClassName() + "(" + lr.getThrown().getCause().getStackTrace()[0].getMethodName() + "-" + lr.getThrown().getCause().getStackTrace()[0].getLineNumber() + ")");
            }
            List<String> st = new ArrayList<String>();
            for (StackTraceElement ste : lr.getThrown().getStackTrace()) {
                String el = " at " + ste.getClassName() + " (" + ste.getMethodName() + ":" + ste.getLineNumber() + ")";
                st.add(el);
            }
            l.setExceptionStacktrace(st);
        }
        l.setLevel(lr.getLevel().getName());
        l.setThreadId(lr.getThreadID());
        if (lr.getParameters() != null) {
            List<String> p = new ArrayList<String>();
            for (Object o : lr.getParameters()) {
                p.add("" + o);
            }
            l.setParams(p);
        }
        l.setTimestamp(lr.getMillis());
        l.setSequence(lr.getSequenceNumber());
        l.setSourceClass(lr.getSourceClassName());
        l.setSourceMethod(lr.getSourceMethodName());
        Morphium.get().store(l);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws SecurityException {

    }


}
