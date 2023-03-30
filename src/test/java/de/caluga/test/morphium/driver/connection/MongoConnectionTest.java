package de.caluga.test.morphium.driver.connection;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.test.DriverMock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class MongoConnectionTest {
    private Logger log = LoggerFactory.getLogger(MongoConnectionTest.class);

    @Test
    public void testSendAndAnswer() throws Exception {
        MongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", "test", "test");
        con.connect(new DriverMock(), "localhost", 27017);
        HelloCommand hello = new HelloCommand(con).setHelloOk(true).setIncludeClient(false);
        var m = con.sendCommand(hello);
        var res = con.readSingleAnswer(m);
        log.info(Utils.toJsonString(res));
        assertNotNull(res);
        assertEquals(1.0, res.get("ok"));
        assertTrue((boolean) res.get("helloOk"));
        con.close();
    }

    @Test
    public void testConnected() throws Exception {
        MongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", "test", "test");
        con.connect(new DriverMock(), "localhost", 27017);
        var m = con.getConnectedTo();
        assertEquals("localhost:27017", m);
        assertEquals("localhost", con.getConnectedToHost());
        assertEquals(27017, con.getConnectedToPort());
        assertTrue(con.isConnected());
        con.close();
    }


    @Test
    public void testWriteRead() throws Exception {
        MongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", "test", "test");
        con.connect(new DriverMock(), "localhost", 27017);
        log.info("Clearing collection");
        ClearCollectionCommand clr = new ClearCollectionCommand(con).setColl("test").setDb("morphium_test");
        int del = clr.doClear();
        log.info("done - deleted " + del + " entries");
        log.info("Storing...");
        List<Map<String, Object>> lst = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            lst.add(Doc.of("value", "test" + i, "counter", i));
        }

        log.info("calling insert");
        InsertMongoCommand insert = new InsertMongoCommand(con).setDocuments(lst).setColl("test").setDb("morphium_test").setBypassDocumentValidation(true);
        var msg = con.sendCommand(insert);
        var ret = con.readSingleAnswer(msg);
        assertTrue(!ret.containsKey("code"));
        log.info("Calling find...");
        FindCommand find = new FindCommand(con).setBatchSize(17).setDb("morphium_test").setColl("test");
        msg = con.sendCommand(find);
        var resultList = con.readAnswerFor(msg);
        assertEquals(1000, resultList.size());
        log.info("Calling find - as cursor");
        msg = con.sendCommand(find);
        var crs = con.getAnswerFor(msg, 100);
        int cnt = 0;

        while (crs.hasNext()) {
            cnt++;
            crs.next();
        }

        assertEquals(1000, cnt);
        con.close();
    }
}
