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

    public VersionMismatchException(Object entityId, long expectedVersion) {
        super("Optimistic locking conflict for entity id=" + entityId
            + ": expected version " + expectedVersion
            + " but document was already modified by another writer");
        this.entityId = entityId;
        this.expectedVersion = expectedVersion;
    }

    /** The {@code @Id} value of the conflicting entity. */
    public Object getEntityId() {
        return entityId;
    }

    /** The version the caller expected to be current in the database. */
    public long getExpectedVersion() {
        return expectedVersion;
    }
}
