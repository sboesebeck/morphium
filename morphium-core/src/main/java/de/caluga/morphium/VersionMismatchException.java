package de.caluga.morphium;

/**
 * Thrown by {@link de.caluga.morphium.writer.MorphiumWriterImpl} when an optimistic-locking
 * conflict is detected during a {@code store} operation on an entity annotated with
 * {@link de.caluga.morphium.annotations.Version}.
 * <p>
 * This means another process has modified (and incremented the version of) the document
 * between the time the entity was read and the time it was stored.
 * The caller should reload the entity and retry the operation.
 * </p>
 */
public class VersionMismatchException extends RuntimeException {

    private final Object entityId;
    private final long expectedVersion;
    private final int listIndex;

    public VersionMismatchException(Object entityId, long expectedVersion) {
        this(entityId, expectedVersion, -1);
    }

    public VersionMismatchException(Object entityId, long expectedVersion, int listIndex) {
        super("Optimistic locking conflict for entity id=" + entityId
            + ": expected version " + expectedVersion
            + " but document was already modified by another writer"
            + (listIndex >= 0 ? " (list index " + listIndex + ")" : ""));
        this.entityId = entityId;
        this.expectedVersion = expectedVersion;
        this.listIndex = listIndex;
    }

    /** The {@code @Id} value of the conflicting entity. */
    public Object getEntityId() {
        return entityId;
    }

    /** The version the caller expected to be current in the database. */
    public long getExpectedVersion() {
        return expectedVersion;
    }

    /**
     * The index of the entity in the list passed to {@code store(List)},
     * or {@code -1} if the exception was thrown from a single-entity store.
     */
    public int getListIndex() {
        return listIndex;
    }
}
