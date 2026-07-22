package de.caluga.test.morphium.driver;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

/**
 * Interim behavior for #262: the in-memory driver cannot emulate time-series collections yet.
 * It used to log a WARN and create a PLAIN collection - a silent divergence from mongod (no
 * timeField enforcement, no retention, listCollections lies about the type). Until #262 lands,
 * the honest answer is a command error (over the wire for PoppyDB clients, an exception for
 * embedded users) instead of a collection that pretends.
 */
@Tag("inmemory")
public class CreateCommandTimeseriesTest {

    private InMemoryDriver drv;

    @BeforeEach
    void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    void tearDown() {
        drv.close();
    }

    @Test
    void createWithTimeseriesFailsLoudly() throws Exception {
        MongoConnection con = drv.getPrimaryConnection(null);
        CreateCommand cmd = new CreateCommand(con).setDb("tsdb").setColl("measurements")
            .setTimeseriesTimeField("ts");

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, cmd::execute,
                "creating a time-series collection must fail loudly, not warn and create a plain one");
        assertTrue(ex.getMessage().toLowerCase().contains("time"),
                "error message should mention time-series: " + ex.getMessage());
        drv.releaseConnection(con);

        List<String> colls = drv.listCollections("tsdb", null);
        assertFalse(colls.contains("measurements"),
                "no plain collection may be left behind by the failed create: " + colls);
    }

    @Test
    void plainCreateStillWorks() throws Exception {
        MongoConnection con = drv.getPrimaryConnection(null);
        CreateCommand cmd = new CreateCommand(con).setDb("tsdb").setColl("plain");
        Map<String, Object> result = cmd.execute();
        assertNotNull(result, "plain create must still succeed");
        drv.releaseConnection(con);
        assertTrue(drv.listCollections("tsdb", null).contains("plain"));
    }
}
