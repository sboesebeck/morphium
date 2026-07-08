package de.caluga.test.morphium.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;

@Tag("driver")
@Tag("external")
public class SingleConnectDriverTests extends DriverTestBase {
    private Logger log = LoggerFactory.getLogger(SingleConnectDriverTests.class);

    @Test
    @Tag("failover")
    public void hostSeedTest() throws Exception {
        var drv = getDriver();
        Thread.sleep(1000);
        if (drv.getHostSeed().size() < 2) {
            log.info("Single-node deployment — skipping hostSeed multi-node test");
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "hostSeedTest requires at least 2 nodes");
        }
        assertThat(drv.getHostSeed().size()).isGreaterThan(1); //should update hostseed

        for (String n : drv.getHostSeed()) {
            log.info("HostSeed: " + n);
        }
    }

    // testHeartbeat (real StepDown failover) moved to SingleConnectDriverFailoverTests
    // - failover tests are manual-only and must not run in CI

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
