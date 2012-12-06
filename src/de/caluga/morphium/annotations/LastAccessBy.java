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
 * <p/>
 * tell morphium to store who last accesed this type. Only works if lastAccessed is set.
 * must be set to a field of type String as well
 *
 * @see de.caluga.morphium.secure.MongoSecurityManager
 */
@Target({FIELD, TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface LastAccessBy {
}
