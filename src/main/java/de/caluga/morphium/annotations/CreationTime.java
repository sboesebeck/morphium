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
 * define the field to store the creation timestamp. Only works ifthis annotation is also added to  the type
 * <code>
 *
 * @CreationTime class Test {
 * @CreationTime private long theTimestamp;
 * ...
 * }
 * </code>
 * if you use a non-MongoId-Field or create the IDs in code, you should set checkForNew to true. Otherwise creationtime won't be set.
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
