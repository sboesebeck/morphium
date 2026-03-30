package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stephan Bösebeck
 * Date: 22.08.12
 * Time: 15:45
 * <p>
 * Store all data found to this field. Field needs to be of type Map&lt;String,Object&gt; - depending on what the mongo-Driver delivers
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AdditionalData {
    /**
     * Indicates whether the additional data field is read only.
     * @return true if field is read only
     */
    boolean readOnly() default true;
}
