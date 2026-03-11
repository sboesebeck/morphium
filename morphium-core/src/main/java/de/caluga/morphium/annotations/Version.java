package de.caluga.morphium.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as the optimistic-locking version counter.
 * <p>
 * The field must be of type {@code long} or {@code Long}.
 * On insert Morphium sets the field to {@code 1}.  On every subsequent
 * update the filter includes the current value and an {@code $inc} atomically
 * increments it.  If the document was modified concurrently (matched count = 0)
 * a {@link de.caluga.morphium.VersionMismatchException} is thrown.
 * </p>
 *
 * <pre>{@code
 * @Entity
 * public class MyEntity {
 *     @Id private String id;
 *     @Version private long version;
 * }
 * }</pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Version {
    /**
     * The MongoDB field name to use.
     * A value of {@code "."} means Morphium derives the name from the Java
     * field name using its standard camelCase convention (same behaviour as
     * {@link Property}).
     */
    String fieldName() default ".";
}
