package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.test.DriverMock;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionTestBase {
    protected final static String coll = "uncached_object";
    protected final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(ConnectionTestBase.class);

    protected SingleMongoConnection getConnection() throws MorphiumDriverException {
        SingleMongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", "test", "test");
        var hello = con.connect(new DriverMock(), "localhost", 27017);
        if (hello.getSaslSupportedMechs() == null || hello.getSaslSupportedMechs().isEmpty()) {
            throw new MorphiumDriverException("Authentication failure!");
        }
        return con;
    }

    @Before
    public void prepare() throws Exception {
        try {
            log.info("Dropping database...");
            var con = getConnection();
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
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
