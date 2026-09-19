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
     * Without a requested read preference the driver's default is used - whatever
     * {@link PooledDriver#getDefaultReadPreference()} answers, this test does not pin that down.
     */
    @Test
    public void withoutARequestedOneTheDriverDefaultIsUsed() {
        try (PooledDriver driver = new PooledDriver()) {
            driver.setDefaultReadPreference(ReadPreference.nearest());

            assertThat(driver.effectiveReadPreference(null).getType())
                .isEqualTo(driver.getDefaultReadPreference().getType());
        }
    }

    /**
     * {@code new ReadPreference()} is public and {@code DriverSettings} tolerates a null type, and
     * the node selection switches on the type - so a typeless preference must never come out of here.
     */
    @Test
    public void aReadPreferenceWithoutATypeIsTreatedLikeNone() {
        try (PooledDriver driver = new PooledDriver()) {
            ReadPreference typeless = new ReadPreference();

            assertThat(driver.effectiveReadPreference(typeless).getType())
                .isNotNull()
                .isEqualTo(driver.getDefaultReadPreference().getType());
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
