package de.caluga.test.morphium.driver.command;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.morphium.driver.DriverTestBase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListCollectionsCommandTest extends DriverTestBase {
    private Logger log = LoggerFactory.getLogger(ListCollectionsCommandTest.class);

    @Test
    public void testListCollections() throws Exception {
        var drv = getDriver();
        MongoConnection con = drv.getConnection();
        new InsertMongoCommand(con).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of("str", "string", "value", 123))).execute();
        new InsertMongoCommand(con).setDb(db).setColl(coll + "_2").setDocuments(Arrays.asList(Doc.of("str", "string", "value", 123))).execute();
        new InsertMongoCommand(con).setDb(db).setColl(coll + "_3").setDocuments(Arrays.asList(Doc.of("str", "string", "value", 123))).execute();
        var lst = drv.listCollections(db, null);
        assertNotNull(lst);
        assertTrue(5 > lst.size() && 0 < lst.size());
        drv.releaseConnection(con);
    }
}
