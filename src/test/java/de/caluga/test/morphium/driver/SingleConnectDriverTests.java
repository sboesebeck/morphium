package de.caluga.test.morphium.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.StepDownCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;

@Disabled
public class SingleConnectDriverTests extends DriverTestBase {
    private Logger log = LoggerFactory.getLogger(SingleConnectDriverTests.class);

    @Test
    public void hostSeedTest() throws Exception {
        var drv = getDriver();
        Thread.sleep(1000);
        assertThat(drv.getHostSeed().size()).isGreaterThan(1); //should update hostseed

        for (String n : drv.getHostSeed()) {
            log.info("HostSeed: " + n);
        }
    }

    @Test
    @Disabled
    public void testHeartbeat() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        log.info("Hearbeat frequency " + drv.getHeartbeatFrequency());
        Thread.sleep(drv.getHeartbeatFrequency() * 5);
        MongoConnection con = drv.getConnection();
        var originalConnectedTo = con.getConnectedTo();
        log.info("Stepping down on node "+originalConnectedTo);
        StepDownCommand cmd = new StepDownCommand(con).setTimeToStepDown(15).setForce(Boolean.TRUE);
        var res = cmd.execute();
        log.info("result: " + Utils.toJsonString(res));
        cmd.releaseConnection();

        while (true) {
            while (!drv.isConnected()) {
                log.info("not connected yet...");
                Thread.sleep(500);
            }

            con = drv.getConnection();

            if (con.getConnectedTo().equals(originalConnectedTo)) {
                log.info("still on same node: "+originalConnectedTo);
                drv.releaseConnection(con);
                Thread.sleep(1000);
                continue;
            }

            String hst = con.getConnectedTo();
            log.info("Failover...to " + con.getConnectedTo());
            boolean br = true;

            for (int i = 0; i < 2; i++) {
                Thread.sleep(1000);

                if (con.getConnectedTo() == null || !con.getConnectedTo().equals(hst)) {
                    log.info("Connection did not stick...");
                    br = false;
                    break;
                }
            }

            if (br) {
                break;
            }
        }

        assertThat(drv.isConnected());
        assertThat(con.getConnectedTo()).isNotEqualTo(originalConnectedTo);
        assertNotNull(originalConnectedTo);
        assertNotNull(con.getConnectedTo());
        log.info("Connection changed from " + originalConnectedTo + " to " + con.getConnectedTo());
        drv.releaseConnection(con);

        while (true) {
            log.info("Waiting for it to change back to..." + originalConnectedTo);
            Thread.sleep(1000);

            if (drv.isConnected()) {
                con = drv.getConnection();

                if (!con.getConnectedTo().equals(originalConnectedTo)) {
                    log.info("Still connected to " + con.getConnectedTo());
                    drv.releaseConnection(con);
                    continue;
                }

                break;
            }
        }

        String hst = con.getConnectedTo();
        log.info("back connected to " + con.getConnectedTo());
        drv.releaseConnection(con);
    }

    @Test
    public void simpleCommandTest() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        HelloCommand cmd = new HelloCommand(drv.getConnection());
        cmd.setIncludeClient(false);
        var result = cmd.execute();
        cmd.releaseConnection();
        cmd = new HelloCommand(drv.getConnection());
        cmd.setIncludeClient(false);
        result = cmd.execute();
        cmd.releaseConnection();
        ClearCollectionCommand clearCmd = new ClearCollectionCommand(drv.getConnection()).setColl("tests").setDb("morphium_test");
        var cmdResult = clearCmd.execute();
        clearCmd.releaseConnection();
        log.info("Got result...");
        InsertMongoCommand insert = new InsertMongoCommand(drv.getConnection()).setDocuments(Arrays.asList(Doc.of("_id", "123123", "value", "thing"))).setColl("tests").setDb("morphium_test");
        //inserted
        var insResult = insert.execute();
        insert.releaseConnection();
    }

    @Test
    public void crudTest() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        ClearCollectionCommand cmd = new ClearCollectionCommand(drv.getConnection()).setColl("tests").setDb("morphium_test");
        var cmdResult = cmd.execute();
        cmd.releaseConnection();
        log.info("Got result...");
        Thread.sleep(5000);
        InsertMongoCommand insert = new InsertMongoCommand(drv.getConnection()).setDocuments(Arrays.asList(Doc.of("_id", "123123", "value", "thing"))).setColl("tests").setDb("morphium_test");
        //inserted
        var insResult = insert.execute();
        insert.releaseConnection();
        var find = new FindCommand(drv.getConnection()).setDb("morphium_test").setColl("tests").setBatchSize(1).setLimit(1).setFilter(Doc.of("_id", "123123"));
        var result = find.execute();
        find.releaseConnection();
        assertNotNull(result);
        assertTrue(result.size() > 0, "did not find");
        var update = new UpdateMongoCommand(drv.getConnection()).setDb("morphium_test").setColl("tests").addUpdate(Doc.of("_id", "123123"), Doc.of("value", "the value"), null, false, false, null,
          null, null);
        var stats = update.execute();
        update.releaseConnection();
        assertNotNull(stats);

        for (var e : stats.entrySet()) {
            log.info("Stat: " + e.getKey() + " --> " + e.getValue());
        }

        find = new FindCommand(drv.getConnection()).setDb("morphium_test").setColl("tests").setBatchSize(1).setLimit(1).setFilter(Doc.of("_id", "123123"));
        result = find.execute();
        find.releaseConnection();
        assertNotNull(result);
        assertTrue(result.size() == 1);
        assertTrue(result.get(0).get("value").equals("the value"), "update failed");
    }

}
