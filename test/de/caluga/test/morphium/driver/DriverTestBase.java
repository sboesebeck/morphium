package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverTestBase {
    protected final static String coll = "uncached_object";
    protected final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(DriverTestBase.class);

    protected SingleMongoConnectDriver getConnection() throws MorphiumDriverException {
        SingleMongoConnectDriver con = new SingleMongoConnectDriver();
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
            var con = getConnection();
            con.connect();
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con.getConnection());
            cmd.setDb(db).execute();
            con.close();
            log.info("done");
        } catch (MorphiumDriverException e) {
            log.error("Error during preparation", e);
            throw new RuntimeException(e);
        }
        log.info("\n     _______.___________.    ___      .______     .___________.\n" +
                "    /       |           |   /   \\     |   _  \\    |           |\n" +
                "   |   (----`---|  |----`  /  ^  \\    |  |_)  |   `---|  |----`\n" +
                "    \\   \\       |  |      /  /_\\  \\   |      /        |  |\n" +
                ".----)   |      |  |     /  _____  \\  |  |\\  \\----.   |  |\n" +
                "|_______/       |__|    /__/     \\__\\ | _| `._____|   |__|\n" +
                ".___________. _______     _______.___________.\n" +
                "|           ||   ____|   /       |           |\n" +
                "`---|  |----`|  |__     |   (----`---|  |----`\n" +
                "    |  |     |   __|     \\   \\       |  |\n" +
                "    |  |     |  |____.----)   |      |  |\n" +
                "    |__|     |_______|_______/       |__|");
    }
}
