package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:31
 * <p/>
 * Define the field to hold the created by string. Usually a user id provided by the security manager
 *
 * @see de.caluga.morphium.secure.MongoSecurityManager
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CreatedBy {
}
