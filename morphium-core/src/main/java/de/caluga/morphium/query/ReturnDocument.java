package de.caluga.morphium.query;

/**
 * Which state of the document {@link UpdateBuilder#findOneAndUpdate(ReturnDocument)} returns. There is deliberately no default: with an upsert that
 * INSERTS, {@code BEFORE} returns {@code null} because there is no pre-image.
 *
 * @apiNote incubating until 6.3, the name and package may change
 */
public enum ReturnDocument {
    /** the document as it was before the update; {@code null} if the update inserted a new document or nothing matched */
    BEFORE,
    /** the document after the update (the inserted document for an upsert that inserted); {@code null} only if nothing matched and there was no upsert */
    AFTER
}
