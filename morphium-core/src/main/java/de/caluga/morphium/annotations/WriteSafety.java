package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 16:31
 * <p>
 * See WriteConcern in MongoDB Java-Driver... for additional information<br>
 * <p>
 * WaitForSync: wait for the write to be synced to disk
 * timeout: set a timeout in ms for the operation - if set to 0, unlimited (default). If set to negative value, wait relative to replication lag
 * level: set the safety level:
 * <ul>
 * <li>{@code IGNORE_ERRORS} None, no checking is done</li>
 * <li>{@code NORMAL} None, network socket errors raised</li>
 * <li>{@code BASIC} Checks server for errors as well as network socket errors raised</li>
 * <li>{@code WAIT_FOR_SLAVE} Checks servers (at lease 2) for errors as well as network socket errors raised</li>
 * <li>{@code MAJORITY} MongoDB {@code w: "majority"} - the recommended durable setting, evaluated server-side</li>
 * <li>{@code WAIT_FOR_ALL_SLAVES} Morphium-specific: {@code w} is set on every write to the number of
 * replica-set members the driver currently considers reachable ({@code MorphiumDriver#getNumReachableHosts()}),
 * so the concern follows the live cluster after a member loss. Caveat: until the driver's heartbeat has
 * noticed the loss (about one heartbeat frequency), writes still carry the old {@code w} and block for
 * {@code timeout}; with {@code timeout = 0} they block indefinitely. Prefer {@code MAJORITY} unless you
 * explicitly want "every available copy".</li>
 * </ul>
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface WriteSafety {
    int timeout() default 0;

    SafetyLevel level() default SafetyLevel.NORMAL;

    boolean waitForJournalCommit() default false;
}
