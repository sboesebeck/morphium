package de.caluga.morphium.annotations.caching;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.13
 * Time: 11:07
 * <p/>
 * TODO: Add documentation here
 */

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface WriteBuffer {
    enum STRATEGY {WRITE, DEL, JUST_WARN,}

    boolean value() default true;

    int size() default 0; //max size of write Buffer entries,0 means unlimited. STRATEGY is meaningless here

    int timeout() default 0;  //use default from morphium, -1 wait till buffer is full, if STRATEGY==DEL && timeout ==-1 nothing will be written!
    // timeout==-1 and size ==0 is not allowed
}
