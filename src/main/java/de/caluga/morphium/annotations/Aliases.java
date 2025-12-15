package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stephan Bösebeck
 * Date: 07.05.12
 * Time: 17:52
 * <p>
 * Define aliases for a field. This way, the same property of an entity might be used with different ways<p>
 * <code>
 * class Data {
 *
 * @Aliases("alias","hugo") private String value;
 * }
 * morphium.createQueryFor(Data.class).f("alias").eq....
 * <p>
 * </code>
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Aliases {
    String[] value() default {};
}
