package jakarta.data.repository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Stand-in for the annotation of Jakarta Data 1.1, which is not on the classpath (the build uses 1.0): the check works on names. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Is {
    Class<?> value() default Object.class;
}
