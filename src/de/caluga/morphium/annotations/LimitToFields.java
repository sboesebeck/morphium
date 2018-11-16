package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;


/**
 * limit to a list of fields, or all fields of a given type
 * <p>
 * sould avoid problems with anonymous sub classes or proxy implementations for example
 * limittofields will not work for fields marked with @Property and a specified fieldname
 * <p>
 * Attention: when limiting to a type, this will include all fields defined in that class, NOT honoring
 * fields that might be ignored by
 *
 * it will be inherited by subclasses, so in subclasses you will need to add all fields manually, use the type
 * or an empty version. Setting type to Object.class uses all fields again.
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface LimitToFields {
    String[] value() default {};

    Class<?> type() default Object.class;
}
