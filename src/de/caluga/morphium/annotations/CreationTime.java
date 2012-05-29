package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:30
 * <p/>
 * TODO: Add documentation here
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CreationTime {
}
