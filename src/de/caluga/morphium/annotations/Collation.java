package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Collation {

    String locale() default "";

    boolean caseLevel() default false;

    String caseFirst() default "";

    int strength() default 3;

    boolean numericOrdering() default false;

    de.caluga.morphium.Collation.Alternate alternate() default de.caluga.morphium.Collation.Alternate.NON_IGNORABLE;

    String maxVariable() default "space";

    boolean backwards() default false;

}
