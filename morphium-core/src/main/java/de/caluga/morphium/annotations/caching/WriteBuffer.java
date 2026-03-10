package de.caluga.morphium.annotations.caching;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.13
 * Time: 11:07
 * <p>
 * write access to entities marked with this annotation will take place buffered. That means, that the write does take
 * place
 */

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface WriteBuffer {
    STRATEGY strategy() default STRATEGY.WAIT;

    /**
     * what to do when max buffer entries is reached
     * WRITE_NEW: write newest entry (synchronous and not add to buffer)
     * WRITE_OLD: write some old entries (and remove from buffer)
     * DEL_OLD: remove old entries from buffer
     * IGNORE_NEW: just ignore incoming
     * JUST_WARN: increase buffer and warn about it
     */
    enum STRATEGY {
        WRITE_NEW, WRITE_OLD, IGNORE_NEW, DEL_OLD, JUST_WARN, WAIT,
    }

    boolean value() default true;

    boolean ordered() default true;

    /**
     * max size of write Buffer entries,0 means unlimited. STRATEGY is meaningless then
     *
     * @return
     */
    int size() default 0; //max size of write Buffer entries,0 means unlimited. STRATEGY is meaningless then

    /**
     * if 0 - use default timeout set in Morphium / morphiumConfig
     * if -1: wait till buffer is full - does not make sense to use with DEL-Strategy
     *
     * @return
     */
    int timeout() default 0;  //use default from morphium, -1 wait till buffer is full
    // timeout==-1 and size ==0 is not allowed
}
