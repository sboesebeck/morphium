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
    private static final boolean AVAILABLE;
    private static final AtomicBoolean warned = new AtomicBoolean(false);

    static {
        boolean found;
        try {
            Class.forName("io.github.classgraph.ClassGraph");
            found = true;
        } catch (ClassNotFoundException e) {
            found = false;
        }
        AVAILABLE = found;
    }

    private ClassGraphHelper() {}

    static boolean isAvailable() {
        return AVAILABLE;
    }

    static void warnIfUnavailable() {
        if (!AVAILABLE && warned.compareAndSet(false, true)) {
            log.warn("ClassGraph not on classpath and no entities pre-registered. "
                + "Runtime classpath scanning is disabled. "
                + "Use EntityRegistry.preRegisterEntities() or add ClassGraph to the classpath.");
        }
    }
}
