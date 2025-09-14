package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.test.OutputHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey.*;

public class DriverTestBase {
    protected final static String coll = "uncached_object";
    protected final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(DriverTestBase.class);

    private SingleMongoConnectDriver driver = null;

    protected SingleMongoConnectDriver getDriver() throws MorphiumDriverException {
        if (driver == null) {
            SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
            drv.setCredentials("admin", "test", "test");
            drv.setMaxWaitTime(1000);
            drv.setHeartbeatFrequency(1000);
            // Be generous during elections/failover in tests
            drv.setRetriesOnNetworkError(30);
            drv.setSleepBetweenErrorRetries(500);

            String hostSeed = System.getenv("HOST_SEED");
            if(hostSeed == null) {
                drv.setHostSeed("localhost:27017", "localhost:27018", "localhost:27019");
            } else {
                drv.setHostSeed(hostSeed.split(","));
            }
            drv.connect();
            driver = drv;
        }
        return driver;
    }

    @AfterEach
    public void check() {
        if (driver != null) {
            Map<MorphiumDriver.DriverStatsKey, Double> driverStats = driver.getDriverStats();
            for (var e : driverStats.entrySet()) {
                log.info("Stats: " + e.getKey() + " -> " + e.getValue());
            }
            if (driverStats.get(MSG_SENT) != (driverStats.get(REPLY_RECEIVED) + driverStats.get(REPLY_IN_MEM))) {
                log.warn("MSG_SENT != REPLY_RECEIVED+REPLY_IN_MEM");
            }
            if (driverStats.get(CONNECTIONS_CLOSED) > driverStats.get(CONNECTIONS_OPENED)) {
                log.error("More connections closed than opened?!?!?");
            } else if (driverStats.get(CONNECTIONS_OPENED) - driverStats.get(CONNECTIONS_CLOSED) > 1) {
                log.warn("More than one connection is still open!");
            }
            if (driverStats.get(CONNECTIONS_BORROWED) - driverStats.get(CONNECTIONS_RELEASED) > 1) {
                log.warn("More than one connection at a time borrowed from pool!");
            }
            if (!driverStats.get(REPLY_PROCESSED).equals(driverStats.get(REPLY_RECEIVED))) {
                log.warn("Some replies were not used");
            }

        }
        driver.close();
        driver = null;


    }

    @BeforeEach
    public void prepare() throws Exception {
        try {
            log.info("Dropping database...");
            var drv=getDriver();
            var con =drv.getConnection();
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
            cmd.setDb(db).execute();
            drv.releaseConnection(con);
            drv.close();
            driver = null;
            log.info("done");
        } catch (MorphiumDriverException e) {
            log.error("Error during preparation", e);
            throw new RuntimeException(e);
        }
        OutputHelper.figletOutput(log,"Start");
        OutputHelper.figletOutput(log,"Test");
    }
}
