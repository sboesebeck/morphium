package de.caluga.morphium.data;

import de.caluga.morphium.query.ReturnDocument;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A Morphium extension of Jakarta Data (NOT portable: the specification has no upsert and no find-and-modify): a repository method that is ONE atomic update
 * command with structured assignments instead of a JSON string. The condition is a JDQL {@code WHERE} expression over named parameters, the assignments name
 * fields of the entity, so a wrong field fails the build.
 *
 * <pre>{@code
 * @AtomicUpdate(where = "id = :id AND (expiresAt < :now OR owner = :owner)",
 *               set = {@Assign(field = "owner", param = "owner"), @Assign(field = "expiresAt", param = "until")},
 *               setOnInsert = @Assign(field = "createdBy", param = "owner"),
 *               inc = @Assign(field = "fencingToken", value = "1"),
 *               upsert = true)
 * Optional<Lease> tryAcquire(@Param("id") String id, @Param("owner") String owner, @Param("now") Date now, @Param("until") Date until);
 * }</pre>
 *
 * <b>The return type chooses the form</b>:
 * <ul>
 * <li>{@code void}, {@code boolean}, {@code int}, {@code long}: the count form, an {@code update} command. The number is the number of documents that were
 * matched or inserted by the upsert; {@code boolean} is "at least one". A lost conditional upsert (the insert collides with the document that the condition
 * excluded: "held by somebody else") is 0 / false, not an exception.</li>
 * <li>the entity or an {@code Optional} of it: the find-and-modify form, a {@code findAndModify} command that returns the document as {@link #returns()} says. The
 * result is null / empty if nothing matched, if the upsert was lost, and for {@link ReturnDocument#BEFORE} if the upsert inserted (there is no document before).
 * An {@link jakarta.data.repository.OrderBy} on the method chooses the document when several match.</li>
 * <li>everything else, collections and streams included, fails the build.</li>
 * </ul>
 * The values are bound through the update builder: field names are translated to the stored names, values are marshalled, a {@code @Version} field is
 * incremented and {@code @LastChange} is set, like for {@code Query.modify()}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(java.lang.annotation.ElementType.METHOD)
public @interface AtomicUpdate {
    /** a JDQL condition over named parameters, with or without the leading {@code WHERE}; empty matches every document */
    String where() default "";

    /** {@code $set} */
    Assign[] set() default {};

    /** {@code $setOnInsert}: only when the upsert inserts */
    Assign[] setOnInsert() default {};

    /** {@code $inc} (the value is a number; use a negative one to decrement) */
    Assign[] inc() default {};

    /** insert a document if none matches */
    boolean upsert() default false;

    /** count form only: update all matching documents instead of one */
    boolean many() default false;

    /** find-and-modify form only: which state of the document to return */
    ReturnDocument returns() default ReturnDocument.AFTER;
}
