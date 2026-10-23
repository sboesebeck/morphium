package de.caluga.morphium.query;

import java.util.Objects;

/**
 * Typed result of {@link UpdateBuilder#tryFindOneAndUpdate(ReturnDocument)}: what the atomic update did, which the document alone cannot tell
 * (a {@code null} document means "nothing matched" as well as "inserted, and there is no pre-image").
 * <ul>
 * <li>{@link Kind#INSERTED}: no document matched and the upsert inserted one; {@code upsertedId} is its {@code _id} (converted to the type of the entity's
 * {@code @Id}); the document is the inserted one for {@link ReturnDocument#AFTER} and {@code null} for {@link ReturnDocument#BEFORE};</li>
 * <li>{@link Kind#UPDATED}: a document matched and was updated (or was matched by an update that changed nothing); the document as the {@code ReturnDocument} says;</li>
 * <li>{@link Kind#NOT_MATCHED}: no document matched and no upsert was requested, nothing was written;</li>
 * <li>{@link Kind#DUPLICATE_KEY}: an upsert whose filter did not match tried to insert and collided with an existing document on a unique key, typically the
 * {@code _id} of the filter. For a conditional upsert on a natural key (a lease) this is the "held by somebody else" case; nothing was written.</li>
 * </ul>
 * A final class so that components can be added without breaking callers.
 *
 * @param <T> the entity type
 * @apiNote incubating until 6.3
 */
public final class UpdateOutcome<T> {

    public enum Kind {
        INSERTED, UPDATED, NOT_MATCHED, DUPLICATE_KEY
    }

    private final Kind kind;
    private final T document;
    private final Object upsertedId;

    public UpdateOutcome(Kind kind, T document, Object upsertedId) {
        if (kind == null) {
            throw new IllegalArgumentException("kind must not be null");
        }

        if (kind == Kind.INSERTED && upsertedId == null) {
            throw new IllegalArgumentException("an insert has an upserted id");
        }

        if (kind != Kind.INSERTED && upsertedId != null) {
            throw new IllegalArgumentException("only an insert has an upserted id, not " + kind);
        }

        if ((kind == Kind.NOT_MATCHED || kind == Kind.DUPLICATE_KEY) && document != null) {
            throw new IllegalArgumentException(kind + " has no document");
        }

        this.kind = kind;
        this.document = document;
        this.upsertedId = upsertedId;
    }

    public static <T> UpdateOutcome<T> duplicateKey() {
        return new UpdateOutcome<>(Kind.DUPLICATE_KEY, null, null);
    }

    public Kind getKind() {
        return kind;
    }

    /** the document as {@link ReturnDocument} selected; {@code null} for NOT_MATCHED, DUPLICATE_KEY and for INSERTED with {@code BEFORE} */
    public T getDocument() {
        return document;
    }

    /** the {@code _id} of the inserted document, only for INSERTED */
    public Object getUpsertedId() {
        return upsertedId;
    }

    /** true if the write took effect: INSERTED or UPDATED */
    public boolean wasApplied() {
        return kind == Kind.INSERTED || kind == Kind.UPDATED;
    }

    public boolean wasInserted() {
        return kind == Kind.INSERTED;
    }

    public boolean isDuplicateKey() {
        return kind == Kind.DUPLICATE_KEY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof UpdateOutcome)) {
            return false;
        }

        UpdateOutcome<?> that = (UpdateOutcome<?>) o;
        return kind == that.kind && Objects.equals(document, that.document) && Objects.equals(upsertedId, that.upsertedId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, document, upsertedId);
    }

    @Override
    public String toString() {
        return "UpdateOutcome{" + kind + (upsertedId != null ? ", upsertedId=" + upsertedId : "") + (document != null ? ", document=" + document : "") + "}";
    }
}
