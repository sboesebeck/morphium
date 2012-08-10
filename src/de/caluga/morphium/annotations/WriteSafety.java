package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 16:31
 * <p/>
 * See WriteConcern in MongoDB Java-Driver... for additional information<br>
 * <p/>
 * </br>
 * WaitForSync: wait for the write to be synced to disk
 * timeout: set a timeout in ms for the operation - if set to 0, unlimited (default)
 * level: set the safety level:
 * <ul>
 * <li>{@code IGNORE_ERRORS} None, no checking is done</li>
 * <li>{@code NORMAL} None, network socket errors raised</li>
 * <li>{@code BASIC} Checks server for errors as well as network socket errors raised</li>
 * <li>{@code WAIT_FOR_SLAVE} Checks servers (at lease 2) for errors as well as network socket errors raised</li>
 * </ul>
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface WriteSafety {
    boolean waitForSync() default false;

    int timeout() default 0;

    SafetyLevel level() default SafetyLevel.NORMAL;

    boolean waitForJournalCommit() default false;
}
