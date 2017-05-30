package de.caluga.morphium;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Vector;

/**
 * Created by stephan on 19.08.16.
 */
public class LoggerRegistry {
    private static LoggerRegistry instance;

    private final List<WeakReference<Logger>> registry;

    private LoggerRegistry() {
        registry = new Vector<>();
    }

    public static LoggerRegistry get() {
        if (instance == null) {
            synchronized (LoggerRegistry.class) {
                if (instance == null) {
                    instance = new LoggerRegistry();
                }
            }
        }
        return instance;
    }

    public void registerLogger(Logger l) {
        WeakReference<Logger> w = new WeakReference<>(l);
        registry.add(w);
    }

    public void updateSettings() {
        //Avoid concurrentmodifiction exception
        for (int i = 0; i < registry.size(); i++) {
            Logger l;
            while ((l = registry.get(i).get()) == null) {
                registry.remove(i++);
                if (i >= registry.size()) {
                    return;
                }
            }
            l.updateSettings();
        }
    }

    public int getNumberOfRegisteredLoggers() {
        for (int i = 0; i < registry.size(); i++) {
            Logger l;
            //noinspection UnusedAssignment
            while ((l = registry.get(i).get()) == null) {
                registry.remove(i++);
                if (i >= registry.size()) {
                    break;
                }
            }

        }
        return registry.size();
    }
}
