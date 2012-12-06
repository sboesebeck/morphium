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
 * <p/>
 * tell morphium to store who last changed elements of this type. Field needs to be string. LastChange needs to be
 * added as well. Put this annotation both to field and type.
 *
 * @see de.caluga.morphium.secure.MongoSecurityManager
 */
@Target({FIELD, TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface LastChangeBy {
}
