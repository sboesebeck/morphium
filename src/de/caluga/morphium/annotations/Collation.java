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

    de.caluga.morphium.Collation.CaseFirst caseFirst() default de.caluga.morphium.Collation.CaseFirst.OFF;

    de.caluga.morphium.Collation.Strength strength() default de.caluga.morphium.Collation.Strength.TERTIARY;

    boolean numericOrdering() default false;

    de.caluga.morphium.Collation.Alternate alternate() default de.caluga.morphium.Collation.Alternate.NON_IGNORABLE;

    de.caluga.morphium.Collation.MaxVariable maxVariable() default de.caluga.morphium.Collation.MaxVariable.SPACE;

    boolean backwards() default false;

}
