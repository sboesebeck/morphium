package de.caluga.morphium.query;

import java.util.Objects;

/**
 * Outcome of {@link UpdateBuilder#updateOne()} / {@link UpdateBuilder#updateMany()}.
 * <ul>
 * <li>{@code matchedCount}: documents the filter selected and that were updated; a document that was inserted by an upsert is not counted;</li>
 * <li>{@code modifiedCount}: documents that really changed (an update that sets the value a document already has modifies nothing);</li>
 * <li>{@code upsertedId}: the {@code _id} of the inserted document, converted to the type of the entity's {@code @Id}, or {@code null};</li>
 * <li>{@code acknowledged}: false if the server did not report counts (an unacknowledged write concern); then all counts are 0 and mean "unknown", not "nothing".</li>
 * </ul>
 * A final class instead of a record so that components can be added without breaking callers.
 *
 * @apiNote incubating until 6.3
 */
public final class UpdateResult {
    private final long matchedCount;
    private final long modifiedCount;
    private final Object upsertedId;
    private final boolean acknowledged;

    public UpdateResult(long matchedCount, long modifiedCount, Object upsertedId, boolean acknowledged) {
        if (matchedCount < 0 || modifiedCount < 0) {
            throw new IllegalArgumentException("counts must not be negative: matched=" + matchedCount + ", modified=" + modifiedCount);
        }

        this.matchedCount = matchedCount;
        this.modifiedCount = modifiedCount;
        this.upsertedId = upsertedId;
        this.acknowledged = acknowledged;
    }

    public static UpdateResult unacknowledged() {
        return new UpdateResult(0, 0, null, false);
    }

    public long getMatchedCount() {
        return matchedCount;
    }

    public long getModifiedCount() {
        return modifiedCount;
    }

    public Object getUpsertedId() {
        return upsertedId;
    }

    /** true if the update inserted a document */
    public boolean wasUpserted() {
        return upsertedId != null;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof UpdateResult)) {
            return false;
        }

        UpdateResult that = (UpdateResult) o;
        return matchedCount == that.matchedCount && modifiedCount == that.modifiedCount && acknowledged == that.acknowledged && Objects.equals(upsertedId, that.upsertedId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matchedCount, modifiedCount, upsertedId, acknowledged);
    }

    @Override
    public String toString() {
        if (!acknowledged) {
            return "UpdateResult{unacknowledged}";
        }

        return "UpdateResult{matched=" + matchedCount + ", modified=" + modifiedCount + ", upsertedId=" + upsertedId + "}";
    }
}
