package de.caluga.morphium.driver.wire;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;

/**
 * The read preference a read is really performed with - this is what the connection is marked with
 * and therefore what ends up in the command's {@code $readPreference}. The PRIMARY-forcing rules
 * were part of {@code getReadConnection()} before and are only reachable for a test since they got
 * their own method.
 */
@Tag("driver")
public class PooledDriverEffectiveReadPreferenceTest {

    @Test
    public void requestedReadPreferenceIsKept() {
        try (PooledDriver driver = new PooledDriver()) {
            assertThat(driver.effectiveReadPreference(ReadPreference.secondary()).getType())
                .isEqualTo(ReadPreferenceType.SECONDARY);
        }
    }

    /**
     * {@link PooledDriver#getDefaultReadPreference()} deliberately overrides the configured default
     * with PRIMARY, so a read without an explicit read preference reads from the primary.
     */
    @Test
    public void withoutARequestedOneThePrimaryIsUsed() {
        try (PooledDriver driver = new PooledDriver()) {
            driver.setDefaultReadPreference(ReadPreference.nearest());

            assertThat(driver.effectiveReadPreference(null).getType()).isEqualTo(ReadPreferenceType.PRIMARY);
        }
    }

    @Test
    public void primaryIsForcedWhileATransactionIsInProgress() {
        try (PooledDriver driver = new PooledDriver()) {
            driver.startTransaction(false);

            try {
                assertThat(driver.effectiveReadPreference(ReadPreference.nearest()).getType())
                    .isEqualTo(ReadPreferenceType.PRIMARY);
            } finally {
                driver.clearTransactionContext();
            }
        }
    }

    @Test
    public void tagSetSurvivesWhenNothingIsForced() {
        try (PooledDriver driver = new PooledDriver()) {
            ReadPreference requested = ReadPreference.secondaryPreferred();
            requested.addTag("dc", "muc");

            assertThat(driver.effectiveReadPreference(requested).getTagSet()).containsEntry("dc", "muc");
        }
    }
}
