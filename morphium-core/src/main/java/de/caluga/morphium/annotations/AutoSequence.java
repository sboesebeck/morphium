package de.caluga.morphium.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field for automatic sequence number assignment via {@link de.caluga.morphium.SequenceGenerator}.
 *
 * <p>When a document is stored and the annotated field is {@code null}, Morphium will
 * automatically assign the next value from the named sequence. If the field already contains
 * a value it is <em>never</em> overwritten.</p>
 *
 * <p><b>Batch optimisation:</b> when an entire list is stored via {@code morphium.storeList()},
 * Morphium allocates all required sequence numbers in a single lock+increment+unlock round-trip
 * (using {@link de.caluga.morphium.SequenceGenerator#getNextBatch(int)}) instead of making one
 * round-trip per document. The caller therefore does not need to know the batch size in advance
 * nor manage a {@link de.caluga.morphium.SequenceGenerator} instance manually.</p>
 *
 * <p><b>Supported field types:</b> {@code long}, {@code Long}, {@code int}, {@code Integer},
 * {@code String}. For primitive types ({@code long}, {@code int}), a field value of {@code 0}
 * is treated as "not yet assigned" — consistent with Java's default initialisation.
 * For boxed types, {@code null} signals "not yet assigned".</p>
 *
 * <p><b>Example:</b></p>
 * <pre>{@code
 * @Entity
 * public class ImportRecord {
 *
 *     @Id
 *     private MorphiumId id;
 *
 *     // Auto-assigned from sequence "import_number", starting at 1, step 1
 *     @AutoSequence(name = "import_number")
 *     private long importNumber;
 *
 *     // ...
 * }
 * }</pre>
 *
 * @see de.caluga.morphium.SequenceGenerator
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AutoSequence {

    /**
     * Name of the MongoDB sequence.
     * <p>Use {@code "."} (the default) to derive the sequence name from the annotated field's
     * MongoDB field name (respecting {@code @Property(fieldName=...)} if present).</p>
     */
    String name() default ".";

    /**
     * First value that will be returned by the sequence when it is created for the first time.
     * Has no effect if the sequence already exists in MongoDB.
     */
    long startValue() default 1;

    /**
     * Step size between consecutive sequence values. Must be non-zero.
     */
    int inc() default 1;
}
