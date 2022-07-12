package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.test.DriverMock;
import de.caluga.test.OutputHelper;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverTestBase {
    protected final static String coll = "uncached_object";
    protected final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(DriverTestBase.class);

    protected SingleMongoConnectDriver getDriver() throws MorphiumDriverException {
        SingleMongoConnectDriver drv=new SingleMongoConnectDriver();
        drv.setMaxWaitTime(1000);
        drv.setHeartbeatFrequency(1000);
        drv.setHostSeed("localhost:27017","localhost:27018","localhost:27019");
        drv.connect();

        return drv;
    }

    @Before
    public void prepare() throws Exception {
        try {
            log.info("Dropping database...");
            var drv=getDriver();
            var con =drv.getConnection();
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
            cmd.setDb(db).execute();
            drv.close();
            log.info("done");
        } catch (MorphiumDriverException e) {
            log.error("Error during preparation", e);
            throw new RuntimeException(e);
        }
        OutputHelper.figletOutput(log,"Start");
        OutputHelper.figletOutput(log,"Test");
    }
}
