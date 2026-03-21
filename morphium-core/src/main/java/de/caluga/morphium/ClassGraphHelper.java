package de.caluga.morphium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Package-private utility that centralises all ClassGraph access behind an availability check.
 * <p>
 * When ClassGraph is not on the classpath (e.g. in a Quarkus native-image build),
 * all scan methods return empty results and log a one-time warning.
 */
final class ClassGraphHelper {
    private static final Logger log = LoggerFactory.getLogger(ClassGraphHelper.class);
    private static volatile boolean available;
    private static final AtomicBoolean warned = new AtomicBoolean(false);

    static {
        available = checkClassGraphPresent();
    }

    private ClassGraphHelper() {}

    static boolean isAvailable() {
        if (available) {
            return true;
        }
        // Re-check: the TCCL may have changed since the static initializer ran
        // (e.g. Quarkus sets its classloader after the extension class is loaded).
        available = checkClassGraphPresent();
        return available;
    }

    private static boolean checkClassGraphPresent() {
        // Try the thread context classloader first (important for Quarkus / isolated classloaders),
        // then fall back to this class's defining classloader.
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl != null) {
            try {
                Class.forName("io.github.classgraph.ClassGraph", false, tccl);
                return true;
            } catch (ClassNotFoundException ignored) {
            }
        }
        try {
            Class.forName("io.github.classgraph.ClassGraph", false, ClassGraphHelper.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException ignored) {
        }
        return false;
    }

    static void warnIfUnavailable() {
        if (!isAvailable() && warned.compareAndSet(false, true)) {
            log.warn("ClassGraph not on classpath and no entities pre-registered. "
                + "Runtime classpath scanning is disabled. "
                + "Use EntityRegistry.preRegisterEntities() or add ClassGraph to the classpath.");
        }
    }
}
