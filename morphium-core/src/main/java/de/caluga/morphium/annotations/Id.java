package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 13:11
 * <p>
 * Specify the ID fiel - needs to be of type Object. is mandatory!
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
}
