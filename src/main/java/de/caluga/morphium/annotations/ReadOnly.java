package de.caluga.morphium.annotations;


import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * mark a property as read only - will only be read from Mongo, but never stored
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ReadOnly {
}
