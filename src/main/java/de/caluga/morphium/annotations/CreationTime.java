package de.caluga.morphium.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:30
 * <p>
 * Define the field to store the creation timestamp. Can be used on field level only,
 * or on both field and type level.
 * <code>
 *
 * class Test {
 * @CreationTime private long theTimestamp;
 * ...
 * }
 * </code>
 * Supported field types: long, Long, Date, LocalDateTime, String.
 * If you use a non-MongoId-Field or create the IDs in code, you should set checkForNew to true. Otherwise creationtime won't be set.
 */
@Target({FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CreationTime {
    /**
     * set it to true to make self-created ids possible
     *
     * @return
     */
    boolean checkForNew() default false;

    /**
     * define the format for creation time when storing into strings
     *
     * @return
     */
    String dateFormat() default "yyyy-MM-dd hh:mm:ss";
}
