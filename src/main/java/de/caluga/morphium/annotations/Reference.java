package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:18
 * <p>
 * Mark this field as a reference to annother mongo object
 * In mongo only the id will be stored here
 * if <code> automaticStore</code> is true (default), objects will be stored if not already done
 */


@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Reference {
    String fieldName() default ".";

    boolean automaticStore() default true;

    boolean lazyLoading() default false;

    String targetCollection() default ".";
}
