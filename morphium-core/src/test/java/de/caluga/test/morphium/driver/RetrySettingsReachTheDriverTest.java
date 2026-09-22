package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code connectionSettings().setRetriesOnNetworkError()} and
 * {@code setSleepBetweenNetworkErrorRetries()} were the two connection settings Morphium never
 * copied onto the driver it builds, so every retry budget in the driver - network errors, the
 * stepdown retries of #393 - ran on the driver's own defaults (5 and 1000 ms) whatever the
 * config said. Seen in PoppyDB's ReplicationManager, which configures 3 and 500 ms and got
 * "retry 5/5" in its log.
 */
@Tag("inmemory")
public class RetrySettingsReachTheDriverTest {

    @Test
    public void retriesAndSleepFromTheConfigAreAppliedToTheDriver() {
        MorphiumConfig cfg = new MorphiumConfig("retry_settings", 10, 10000, 1000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        cfg.connectionSettings().setRetriesOnNetworkError(3);
        cfg.connectionSettings().setSleepBetweenNetworkErrorRetries(250);

        try (Morphium morphium = new Morphium(cfg)) {
            assertEquals(3, morphium.getDriver().getRetriesOnNetworkError(), "retriesOnNetworkError must reach the driver");
            assertEquals(250, morphium.getDriver().getSleepBetweenErrorRetries(), "sleepBetweenNetworkErrorRetries must reach the driver");
        }
    }

    /** A config that says nothing keeps what every client ran with before the wiring: 5 and 100 ms. */
    @Test
    public void unconfiguredDefaultsAreTheDriversOwn() {
        MorphiumConfig cfg = new MorphiumConfig("retry_settings", 10, 10000, 1000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);

        try (Morphium morphium = new Morphium(cfg)) {
            assertEquals(5, morphium.getDriver().getRetriesOnNetworkError());
            assertEquals(100, morphium.getDriver().getSleepBetweenErrorRetries());
        }
    }
}
