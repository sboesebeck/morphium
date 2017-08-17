package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;


/**
 * List of fields in class, that can be ignored. Defaults no none.
 * usually an exact match, but can use ~ as substring, / as regex marker
 * <p>
 * Field names are JAVA Fields, not translated ones for mongo
 * <p>
 * IgnoreFields will not be honored for fields marked with @Property and a custom fieldname
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface IgnoreFields {
    String[] value() default {};
}
