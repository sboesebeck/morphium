package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 16:43
 * <p>
 * Mark an object to be used only embedded in an other object. THIS MUST NOT BE USED TOGETHER WITH @Entity!!!!
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Embedded {
    boolean translateCamelCase() default true;

    String typeId() default ".";

    /**
     * several different objects of same type stored in field
     * if set, className is  stored in object
     *
     * @return polymorph usage
     */

    boolean polymorph() default false;
}
