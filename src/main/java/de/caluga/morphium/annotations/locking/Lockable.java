package de.caluga.morphium.annotations.locking;



import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Entities marked with that annotation can be locked!
 * Attention: lockedBy-Field needs to be specified
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Lockable {
}
