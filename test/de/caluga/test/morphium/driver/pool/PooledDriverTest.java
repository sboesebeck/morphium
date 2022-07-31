package de.caluga.test.morphium.driver.pool;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.View;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PooledDriverTest {
    private Logger log = LoggerFactory.getLogger(PooledDriverTest.class);

    @Test
    public void testPooledConnections() throws Exception {
        PooledDriver drv = getDriver();
        log.info("Connecting...");
        drv.connect();
        Thread.sleep(1000);
        log.info("Checking status");
        for (var e : drv.getNumConnectionsByHost().entrySet()) {
            log.info("Host: " + e.getKey() + " connections: " + e.getValue());
            assertThat(e.getValue()).isLessThanOrEqualTo(10).isGreaterThanOrEqualTo(2);
        }
        drv.close();
    }

    @Test
    public void testCRUD() throws Exception {
        var drv = getDriver();
        try (drv) {
            drv.connect();
            while (!drv.isConnected()) {
                log.info("Waiting...");
                Thread.sleep(500);
            }

            //dropping testdb
            var con = drv.getPrimaryConnection(null);
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
            cmd.setDb("morphium_test");
            cmd.execute();

            con.release();
            ObjectMapperImpl om = new ObjectMapperImpl();
            //Write
            for (int i = 0; i < 100; i++) {
                var c = drv.getPrimaryConnection(null);
                InsertMongoCommand insert = new InsertMongoCommand(c);
                insert.setDocuments(Arrays.asList(
                        om.serialize(new UncachedObject("value_" + (i + 100), i + 100)),
                        om.serialize(new UncachedObject("value_" + i, i))
                ));
                insert.setColl("uncached_object").setDb("morphium_test");
                var m = insert.execute();
                assertEquals("should insert 2 elements", 2, m.get("n"));
                log.info(Utils.toJsonString(m));
                c.release();
            }
//            log.info("Waiting for reconnects due to idle / max age connections");
//            Thread.sleep(1000); //forcing idle reconnects

            //reading
            for (int i = 0; i < 100; i++) {
                var c = drv.getReadConnection(ReadPreference.secondaryPreferred());
                log.info("got connection to " + c.getConnectedTo());
                FindCommand fnd = new FindCommand(c).setLimit(10).setColl("uncached_object").setDb("morphium_test")
                        .setFilter(Doc.of("counter", i));
                var ret = fnd.execute();
                assertNotNull(ret);
                assertEquals("Should find exactly one element", ret.size(), 1);
                c.release();
            }
        }


    }

    private PooledDriver getDriver() throws MorphiumDriverException {
        var drv = new PooledDriver();
        drv.setHostSeed("localhost:27017", "localhost:27018", "localhost:27019");
        drv.setMaxConnectionsPerHost(10);
        drv.setMinConnectionsPerHost(2);
        drv.setConnectionTimeout(2000);
        drv.setMaxConnectionLifetime(1000);
        drv.setMaxConnectionIdleTime(500);

        return drv;
    }


}
