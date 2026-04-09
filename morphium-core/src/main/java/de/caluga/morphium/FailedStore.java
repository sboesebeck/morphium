package de.caluga.morphium;

/**
 * Represents a single failure from a {@code storeList()} call with
 * {@code continueOnError=true}.
 *
 * @param <T> the entity type
 */
public class FailedStore<T> {

    private final int index;
    private final T entity;
    private final Exception cause;

    public FailedStore(int index, T entity, Exception cause) {
        this.index = index;
        this.entity = entity;
        this.cause = cause;
    }

    /** The index of the entity in the original list. */
    public int getIndex() {
        return index;
    }

    /** The entity that failed to store. */
    public T getEntity() {
        return entity;
    }

    /** The exception that caused the failure (typically {@link VersionMismatchException}). */
    public Exception getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "FailedStore{index=" + index + ", entity=" + entity + ", cause=" + cause.getMessage() + "}";
    }
}
