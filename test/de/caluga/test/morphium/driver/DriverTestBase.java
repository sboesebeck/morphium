package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.sync.SingleMongoConnection;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverTestBase {
    protected final static String coll = "uncached_object";
    protected final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(DriverTestBase.class);

    protected SingleMongoConnection getSynchronousMongoConnection() throws MorphiumDriverException {
        SingleMongoConnection con = new SingleMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setDefaultBatchSize(5);
        con.setMaxWaitTime(100000);
        con.setConnectionTimeout(1000);
        //con.connect();
//        log.info("Connected");
        return con;
    }

    @Before
    public void prepare() throws Exception {
        try {
            log.info("Dropping database...");
            var con = getSynchronousMongoConnection();
            con.connect();
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
            cmd.setDb(db).execute();
            con.disconnect();
            log.info("done");
        } catch (MorphiumDriverException e) {
            log.error("Error during preparation", e);
            throw new RuntimeException(e);
        }
    }
}
