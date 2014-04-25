package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * mark a property as write only. It will not be read from mongo, when unmarshalling, but will be stored, if set
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface WriteOnly {
}
