package de.caluga.morphium.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;

/**
 * User: Stephan Bösebeck
 * Date: 10.04.12
 * Time: 22:29
 * <p/>
 * used to tell morphium which field a setter does manipulate - needed for partially updates.
 * only necessary, if field name differs from setter... e.g. if setter is setTheValue and the field is called theValue, the annotation is not needed
 */
@Target(value = {METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface PartialUpdate {
    String value() default ".";
}
