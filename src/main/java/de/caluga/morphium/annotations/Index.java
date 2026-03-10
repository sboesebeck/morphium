package de.caluga.morphium.annotations;

import de.caluga.morphium.Collation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Defines indices to be created for a collection when `morphium.ensureIndicesFor` is called for the corresponding entity.
 * This annotation can be used on a field to create a single-field index, or on the class to create compound indexes.
 * </p>
 * <h3>Single-Field Indexes</h3>
 * <p>
 * To create a single-field index, annotate the field with `@Index`. By default, the index will be in ascending order.
 * To create a descending index, use `@Index(decrement = true)`.
 * </p>
 * <pre>
 * {@code
 * @Entity
 * public class MyClass {
 *     @Index
 *     private String myField;
 *
 *     @Index(decrement = true)
 *     private String timestamp;
 * }
 * }
 * </pre>
 *
 * <h3>Compound Indexes</h3>
 * <p>
 * To create a compound index, annotate the class with `@Index` and provide a list of fields to include in the index.
 * The order of the fields is important. To specify the direction of the index for each field, prefix the field name
 * with a `-` for descending order.
 * </p>
 * <pre>
 * {@code
 * @Entity
 * @Index({"name, -timestamp"})
 * public class MyClass {
 *     private String name;
 *     private long timestamp;
 * }
 * }
 * </pre>
 *
 * <h3>Geospatial Indexes</h3>
 * <p>
 * Morphium also supports 2D geospatial indexes. For more information, please refer to the
 * <a href="http://docs.mongodb.org/manual/applications/2d/">MongoDB documentation</a>.
 * </p>
 */
@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    boolean decrement() default false;

    String[] value() default {};

    String[] options() default {};

    String locale() default "";

    boolean caseLevel() default false;

    Collation.CaseFirst caseFirst() default Collation.CaseFirst.OFF;

    Collation.Strength strength() default Collation.Strength.TERTIARY;

    boolean numericOrdering() default false;

    de.caluga.morphium.Collation.Alternate alternate() default de.caluga.morphium.Collation.Alternate.NON_IGNORABLE;

    Collation.MaxVariable maxVariable() default Collation.MaxVariable.SPACE;

    boolean backwards() default false;
}
