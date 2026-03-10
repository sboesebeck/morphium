package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * Created by stephan on 09.07.14.
 */
@SuppressWarnings("DefaultFileTemplate")
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Capped {
    int maxSize();

    int maxEntries();

}
