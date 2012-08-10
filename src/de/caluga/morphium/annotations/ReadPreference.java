package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 12:55
 * <p/>
 * TODO: Add documentation here
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ReadPreference {
    ReadPreferenceLevel value() default ReadPreferenceLevel.ALL_NODES;
}
