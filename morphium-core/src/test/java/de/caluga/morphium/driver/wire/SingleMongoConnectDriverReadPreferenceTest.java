package de.caluga.morphium.driver.wire;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;

/**
 * What the single connection is stamped with, now that the stamp reaches the wire. This driver is
 * a direct connection: on a deliberate secondary (ConnectionType SECONDARY or ANY) a read sent as
 * {@code mode: "primary"} is answered with 13435 by mongod and PoppyDB alike, so the server
 * selection spec's rule for direct connections applies and a primary read goes out as
 * {@code primaryPreferred}.
 */
@Tag("driver")
public class SingleMongoConnectDriverReadPreferenceTest {

    @Test
    public void onAPrimaryConnectionAPrimaryReadIsSentAsPrimary() {
        SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
        driver.setConnectionType(ConnectionType.PRIMARY);

        assertThat(driver.readPreferenceForDirectConnection(ReadPreference.primary()).getType())
            .isEqualTo(ReadPreferenceType.PRIMARY);
    }

    @Test
    public void onASecondaryConnectionAPrimaryReadIsSentAsPrimaryPreferred() {
        SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
        driver.setConnectionType(ConnectionType.SECONDARY);

        assertThat(driver.readPreferenceForDirectConnection(ReadPreference.primary()).getType())
            .isEqualTo(ReadPreferenceType.PRIMARY_PREFERRED);
    }

    @Test
    public void onAnAnyConnectionAPrimaryReadIsSentAsPrimaryPreferred() {
        SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
        driver.setConnectionType(ConnectionType.ANY);

        assertThat(driver.readPreferenceForDirectConnection(ReadPreference.primary()).getType())
            .isEqualTo(ReadPreferenceType.PRIMARY_PREFERRED);
    }

    @Test
    public void anythingButPrimaryIsSentUnchanged() {
        SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
        driver.setConnectionType(ConnectionType.SECONDARY);
        ReadPreference nearest = ReadPreference.nearest();
        nearest.addTag("dc", "muc");

        assertThat(driver.readPreferenceForDirectConnection(nearest)).isSameAs(nearest);
    }

    /**
     * The PRIMARY-forcing rules used to live in PooledDriver only; a transaction on this driver sent
     * whatever was requested - {@code mode: "nearest"} to a mongos inside a transaction, which it
     * rejects. The rules are shared now.
     */
    @Test
    public void primaryIsForcedWhileATransactionIsInProgress() {
        SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
        driver.startTransaction(false);

        try {
            assertThat(driver.effectiveReadPreference(ReadPreference.nearest()).getType())
                .isEqualTo(ReadPreferenceType.PRIMARY);
        } finally {
            driver.clearTransactionContext();
        }
    }
}
