package de.caluga.morphium.query;

/**
 * Which state of the document {@link UpdateBuilder#findOneAndUpdate(ReturnDocument)}, {@link FindAndModifyOptions} and {@link Query#findOneAndUpdate(java.util.Map, FindAndModifyOptions)}
 * return. There is deliberately no default: with an upsert that INSERTS, {@code BEFORE} returns {@code null} because there is no pre-image - the same {@code null} as for
 * "nothing matched"; {@link UpdateBuilder#tryFindOneAndUpdate(ReturnDocument)} tells the two apart. A real MongoDB and the InMemoryDriver behave the same.
 *
 * @apiNote incubating in 7.0, may change in a later 7.x release, the name and package may change
 */
public enum ReturnDocument {
    /** the document as it was before the update; {@code null} if the update inserted a new document or nothing matched */
    BEFORE,
    /** the document after the update (the inserted document for an upsert that inserted); {@code null} only if nothing matched and there was no upsert */
    AFTER
}
