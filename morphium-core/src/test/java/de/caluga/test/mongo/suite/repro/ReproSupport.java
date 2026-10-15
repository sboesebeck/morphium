package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.driver.MorphiumDriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Small helpers shared by the reproduction tests in this package. The tests document how raw update
 * documents behave on a real MongoDB versus the InMemoryDriver; the helpers capture an outcome (a value
 * or a full exception chain with Mongo error codes) in a form that is easy to assert and to log.
 */
final class ReproSupport {
    private static final Logger log = LoggerFactory.getLogger(ReproSupport.class);

    private ReproSupport() {
    }

    /** Result of running a call: either a value or the thrown exception. */
    static final class Outcome<T> {
        final T value;
        final Throwable error;

        Outcome(T value, Throwable error) {
            this.value = value;
            this.error = error;
        }

        boolean failed() {
            return error != null;
        }

        /** Finds the first exception of the given type in the cause chain, or null. */
        <E extends Throwable> E find(Class<E> type) {
            for (Throwable t = error; t != null; t = t.getCause()) {
                if (type.isInstance(t)) {
                    return type.cast(t);
                }
            }
            return null;
        }

        String describe() {
            return error == null ? "OK value=" + value : ReproSupport.describe(error);
        }
    }

    @FunctionalInterface
    interface Call<T> {
        T call() throws Exception;
    }

    static <T> Outcome<T> attempt(Call<T> call) {
        try {
            return new Outcome<>(call.call(), null);
        } catch (Throwable t) {
            return new Outcome<>(null, t);
        }
    }

    /** Whole cause chain as "Type[mongoCode=..]: message <- Type: message ...". */
    static String describe(Throwable t) {
        List<String> parts = new ArrayList<>();
        for (Throwable c = t; c != null && parts.size() < 8; c = c.getCause()) {
            String s = c.getClass().getName();
            if (c instanceof MorphiumDriverException mde) {
                s += "[mongoCode=" + mde.getMongoCode() + ", mongoReason=" + mde.getMongoReason()
                     + (mde.getWriteErrors() == null ? "" : ", writeErrors=" + mde.getWriteErrors()) + "]";
            }
            parts.add(s + ": " + c.getMessage());
        }
        return String.join("  <-  ", parts);
    }

    /** Emits a greppable line which the reproduction notes are based on. */
    static void report(String driver, String hypothesis, String what) {
        log.info("REPRO-RESULT [{}] {}: {}", driver, hypothesis, what);
    }
}
