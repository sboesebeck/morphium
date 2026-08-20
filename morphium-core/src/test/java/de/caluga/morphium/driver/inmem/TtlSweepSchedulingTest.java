package de.caluga.morphium.driver.inmem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The TTL sweep is scheduled by {@code scheduleExpire()}, which is reached from two places:
 * {@code setExpireCheck()} and {@code connect()}. Configuring the interval before connecting -
 * the documented way to do it, since the period is fixed when the task is scheduled - therefore
 * schedules the sweep twice, and the first task is only ever referenced by the {@code expire}
 * field that the second one overwrites: it can never be cancelled again and keeps sweeping for
 * the lifetime of the driver.
 *
 * <p>Harmless in its effects (the sweep is idempotent), but it is a leaked periodic task, and
 * since #325 both copies land on the same single TTL thread.
 */
@Tag("inmemory")
public class TtlSweepSchedulingTest {
    private InMemoryDriver drv;

    @AfterEach
    void tearDown() {
        if (drv != null) {
            drv.shutdown(true);
            drv = null;
        }
    }

    @Test
    void configuringTheExpiryIntervalBeforeConnectMustLeaveExactlyOneSweepScheduled() {
        drv = new InMemoryDriver();
        // The documented order: the period is fixed when the task is scheduled, so it has to be
        // set before connecting - and connect() schedules again.
        drv.setExpireCheck(3_600_000);
        drv.connect();
        assertEquals(1, drv.scheduledTtlTasks(),
            "setExpireCheck() before connect() must not leave a second, unreferenced sweep task "
            + "behind - connect() reschedules, and whatever the expire field pointed at before "
            + "that can never be cancelled again");
    }

    @Test
    void changingTheExpiryIntervalOnAConnectedDriverMustReplaceTheRunningSweep() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setExpireCheck(3_600_000);
        drv.setExpireCheck(1_800_000);
        assertEquals(1, drv.scheduledTtlTasks(),
            "rescheduling on a connected driver must replace the running sweep, not add to it");
    }
}
