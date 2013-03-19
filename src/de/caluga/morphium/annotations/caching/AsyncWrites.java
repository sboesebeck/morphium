package de.caluga.morphium.annotations.caching;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 12:38
 * <p/>
 * TODO: Add documentation here
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface AsyncWrites {
    boolean value() default true;
}
