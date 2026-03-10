package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 12:55
 * <p>
 * Set the default read preference level for this type. It can be changed by query, if necessary
 *
 * @see de.caluga.morphium.Morphium
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultReadPreference {
    ReadPreferenceLevel value() default ReadPreferenceLevel.NEAREST;
}
