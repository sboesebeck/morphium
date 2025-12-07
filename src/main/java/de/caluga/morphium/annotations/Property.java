package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:14
 * <p>
 * Define this field to be a Property. Usually not necessary, as all fields are Properties by default.
 * But with this annotation, the name of the field in mongo can be changed.
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Property {
    String fieldName() default ".";

}
