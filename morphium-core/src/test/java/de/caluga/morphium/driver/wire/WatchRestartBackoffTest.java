package de.caluga.morphium.driver.wire;

import static de.caluga.morphium.driver.wire.SingleMongoConnection.WATCH_RESTART_BACKOFF_MAX_MS;
import static de.caluga.morphium.driver.wire.SingleMongoConnection.WATCH_RESTART_BACKOFF_MIN_MS;
import static de.caluga.morphium.driver.wire.SingleMongoConnection.watchRestartBackoffMs;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The pacing of watch()'s in-place restart after an exhausted cursor (#383): the first restart
 * stays immediate - a one-off exhaustion resumes exactly as before - and only repeats in a row
 * are slowed down, doubling with jitter up to a cap.
 */
@Tag("core")
public class WatchRestartBackoffTest {

    @Test
    public void firstRestartIsImmediate() {
        assertThat(watchRestartBackoffMs(0)).isZero();
    }

    @Test
    public void repeatsDoubleWithJitterFromMinToMax() {
        for (int n = 1; n <= 12; n++) {
            long step = Math.min(WATCH_RESTART_BACKOFF_MIN_MS << (n - 1), WATCH_RESTART_BACKOFF_MAX_MS);

            for (int sample = 0; sample < 50; sample++) {
                assertThat(watchRestartBackoffMs(n))
                        .as("restart #%d: half the step fixed, half random", n)
                        .isBetween(step / 2, step);
            }
        }
    }

    @Test
    public void capHoldsForAbsurdCounts() {
        assertThat(watchRestartBackoffMs(Integer.MAX_VALUE)).isBetween(WATCH_RESTART_BACKOFF_MAX_MS / 2, WATCH_RESTART_BACKOFF_MAX_MS);
    }
}
