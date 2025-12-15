package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:32
 * <p>
 * Tell the type to store the last access. Field is specified by the same annotation at field level.
 * <b>Attention:</b> this causes one write access for <em>every</em> read!
 */
@Target({FIELD, TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface LastAccess {
    /**
     * define the format for access time when storing into strings
     * <b>Careful: this will slow down reading</b>
     *
     * @return
     */
    String dateFormat() default "yyyy-MM-dd hh:mm:ss";
}
