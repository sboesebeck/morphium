package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;


/**
 * used to mark a field for versioning
 * see Entity-Annotation
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Version {
}
