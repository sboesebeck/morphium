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
 * <p/>
 * define the field to store the creation timestamp. Only works ifthis annotation is also added to  the type
 * <code>
 *
 * @CreationTime class Test {
 * @CreationTime private long theTimestamp;
 * ...
 * }
 * </code>
 */
@Target({FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CreationTime {
}
