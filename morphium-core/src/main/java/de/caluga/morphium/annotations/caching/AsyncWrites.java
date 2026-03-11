package de.caluga.morphium.annotations.caching;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 12:38
 * <p>
 * When this annotation is added to a class, all store or update calls of objects of this class will be asynchrounously
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface AsyncWrites {
    boolean value() default true;
}
