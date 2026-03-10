package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:31
 * <p>
 * Tell Morphium to store the timestamp of the last change. Can be used on field level only,
 * or on both field and type level.
 * Supported field types: long, Long, Date, LocalDateTime, String.
 */
@Target({FIELD, TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface LastChange {
    /**
     * define the format for timestamp when storing into strings
     *
     * @return
     */
    String dateFormat() default "yyyy-MM-dd hh:mm:ss";
}
