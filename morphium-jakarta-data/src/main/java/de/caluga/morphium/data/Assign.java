package de.caluga.morphium.data;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * One field assignment of an {@link AtomicUpdate}: the {@link #field() field} and a value, either the argument of a method parameter ({@link #param()}, by the name
 * of the parameter) or a literal ({@link #value()}: a number, a string in single quotes, {@code TRUE} or {@code FALSE}). Exactly one of the two is given.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Assign {
    /** the field of the entity (the Java name; a dotted path starts with a field of the entity) */
    String field();

    /** the name of the method parameter that holds the value */
    String param() default "";

    /** a literal value: {@code 1}, {@code 2.5}, {@code 'text'}, {@code TRUE} */
    String value() default "";
}
