package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stephan Bösebeck
 * Date: 07.05.12
 * Time: 17:52
 * <p/>
 * TODO: Add documentation here
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Aliases {
    String[] value() default {};
}
