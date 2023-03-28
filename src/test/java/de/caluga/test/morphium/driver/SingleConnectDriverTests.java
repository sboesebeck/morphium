package de.caluga.test.morphium.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.commands.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;


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
    public void testHeartbeat() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        log.info("Hearbeat frequency " + drv.getHeartbeatFrequency());
        Thread.sleep(500);
        MongoConnection con = drv.getConnection();
        var originalConnectedTo = con.getConnectedTo();
        StepDownCommand cmd = new StepDownCommand(con).setTimeToStepDown(15).setForce(Boolean.TRUE);
        var res = cmd.execute();
        log.info("result: " + Utils.toJsonString(res));
        con.release();
        while (true) {
            while (!drv.isConnected()) {
                //log.info("Connected: "+drv.isConnected()+" "+con.getConnectedTo());
                //Thread.sleep(500);
                Thread.yield();

            }
            con = drv.getConnection();
            if (con.getConnectedTo().equals(originalConnectedTo)) {
                con.release();
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
            if (br)
                break;
        }
        assertThat(drv.isConnected());
        assertThat(con.getConnectedTo()).isNotEqualTo(originalConnectedTo);
        assertNotNull(originalConnectedTo);
        assertNotNull(con.getConnectedTo());

        log.info("Connection changed from " + originalConnectedTo + " to " + con.getConnectedTo());

        con.release();
        while (true) {
            log.info("Waiting for it to change back...");
            Thread.sleep(1000);
            if (drv.isConnected()) {
                con = drv.getConnection();
                if (!con.getConnectedTo().equals(originalConnectedTo)) {
                    con.release();
                    continue;
                }
                break;
            }
        }

        String hst = con.getConnectedTo();
        log.info("back connected to " + con.getConnectedTo());
        con.release();

    }

    // @Test
    // public void testFailover() throws Exception {
    //     SingleMongoConnectDriver drv = getDriver();
    //     log.info("Hearbeat frequency " + drv.getHeartbeatFrequency());
    //     while (!drv.isConnected()) Thread.yield();
    //     MongoConnection con = drv.getConnection();
    //     var originalConnectedTo = con.getConnectedTo();
    //     ShutdownCommand cmd = new ShutdownCommand(con).setForce(Boolean.TRUE).setTimeoutSecs(2);
    //     var res = cmd.executeAsync();
    //     log.info("Shutdown command sent..."+res);
    //     con.release();
    //     Thread.sleep(5000); //shutdown takes a while
    //     while (true) {
    //         while (!drv.isConnected()) {
    //             Thread.yield();
    //         }
    //         con = drv.getConnection();
    //         if (con.getConnectedTo() == null || con.getConnectedTo().equals(originalConnectedTo)) {
    //             continue;
    //         }
    //         String hst = con.getConnectedTo();
    //         log.info("Failover...to " + con.getConnectedTo());
    //         boolean br = true;
    //         for (int i = 0; i < 2; i++) {
    //             Thread.sleep(1000);
    //             if (con.getConnectedTo() == null || !con.getConnectedTo().equals(hst)) {
    //                 log.info("Connection did not stick...");
    //                 br = false;
    //                 break;
    //             }
    //         }
    //         if (br)
    //             break;
    //     }
    //
    //     assertThat(drv.isConnected());
    //     assertThat(con.getConnectedTo()).isNotEqualTo(originalConnectedTo);
    //     assertNotNull(originalConnectedTo);
    //     assertNotNull(con.getConnectedTo());
    //     log.info("Connection changed from " + originalConnectedTo + " to " + con.getConnectedTo());
    //     con.release();
    //     log.info("HostSeed:");
    //     for (var c : drv.getHostSeed()) {
    //         log.info("---> " + c);
    //     }
    //
    //     String startNode = MorphiumTestBase.getProps().getProperty("failoverNodeRestartCommand");
    //     if (startNode == null) {
    //         log.error("Cannot restart node for tests! Please do so manually!");
    //         Thread.sleep(5000);
    //     } else {
    //         log.info("Trying to restart shut down node! using " + startNode);
    //         var ret = Runtime.getRuntime().exec(startNode);
    //         BufferedReader br = new BufferedReader(new InputStreamReader(ret.getInputStream()));
    //         while (ret.isAlive()) {
    //
    //             if (br.ready()) {
    //                 log.info(br.readLine());
    //             } else {
    //                 log.info("waiting for node to startup...");
    //             }
    //             Thread.sleep(1000);
    //         }
    //
    //     }
    //     log.info("Node should be started up... checking cluster");
    //
    //     OUT:
    //     while (true) {
    //         Thread.sleep(1000);
    //         con = drv.getConnection();
    //         ReplicastStatusCommand rstat = new ReplicastStatusCommand(con);
    //         var status = rstat.execute();
    //         con.release();
    //         var members = ((List<Map<String, Object>>) status.get("members"));
    //         for (Map<String, Object> mem : members) {
    //             if (mem.get("name").equals(originalConnectedTo)) {
    //                 Object stateStr = mem.get("stateStr");
    //                 log.info("Status of original host: " + stateStr);
    //                 if (stateStr.equals("PRIMARY")) {
    //                     log.info("Finally...");
    //                     break OUT;
    //                 } else if (stateStr.equals("STARTUP") || stateStr.equals("STARTUP2") || stateStr.equals("SECONDARY")) {
    //                     log.info("Status is still " + stateStr);
    //                 } else {
    //                     log.warn("Unknown status " + stateStr);
    //                 }
    //             }
    //         }
    //     }
    //     log.info("State recovered");
    //
    // }


    @Test
    public void simpleCommandTest() throws Exception {
        SingleMongoConnectDriver drv=getDriver();
        HelloCommand cmd=new HelloCommand(drv.getConnection());
        cmd.setIncludeClient(false);
        var result=cmd.execute();
        cmd.releaseConnection();

        cmd=new HelloCommand(drv.getConnection());
        cmd.setIncludeClient(false);
        result=cmd.execute();
        cmd.releaseConnection();
        ClearCollectionCommand clearCmd= new ClearCollectionCommand(drv.getConnection()).setColl("tests").setDb("morphium_test");
        var cmdResult=clearCmd.execute();
        clearCmd.releaseConnection();
        log.info("Got result...");

        InsertMongoCommand insert = new InsertMongoCommand(drv.getConnection()).setDocuments(Arrays.asList(Doc.of("_id", "123123", "value", "thing")))
                .setColl("tests")
                .setDb("morphium_test");
        //inserted
        var insResult=insert.execute();
        insert.releaseConnection();

    }

    @Test
    public void crudTest() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        ClearCollectionCommand cmd = new ClearCollectionCommand(drv.getConnection()).setColl("tests").setDb("morphium_test");
        var cmdResult=cmd.execute();
        cmd.releaseConnection();
        log.info("Got result...");
        Thread.sleep(5000);
        InsertMongoCommand insert = new InsertMongoCommand(drv.getConnection()).setDocuments(Arrays.asList(Doc.of("_id", "123123", "value", "thing")))
                .setColl("tests")
                .setDb("morphium_test");
        //inserted
        var insResult=insert.execute();
        insert.releaseConnection();
        var find = new FindCommand(drv.getConnection()).setDb("morphium_test").setColl("tests")
                .setBatchSize(1)
                .setLimit(1)
                .setFilter(Doc.of("_id", "123123"));
        var result = find.execute();
        find.releaseConnection();
        assertNotNull(result);
        assertTrue(result.size() > 0,"did not find");

        var update = new UpdateMongoCommand(drv.getConnection()).setDb("morphium_test").setColl("tests")
                .addUpdate(Doc.of("_id", "123123"), Doc.of("value", "the value"), null, false, false, null, null, null);
        var stats = update.execute();
        update.releaseConnection();
        assertNotNull(stats);
        for (var e : stats.entrySet()) {
            log.info("Stat: " + e.getKey() + " --> " + e.getValue());
        }
        find = new FindCommand(drv.getConnection()).setDb("morphium_test").setColl("tests")
                .setBatchSize(1)
                .setLimit(1)
                .setFilter(Doc.of("_id", "123123"));
        result = find.execute();
        find.releaseConnection();
        assertNotNull(result);
        assertTrue(result.size() == 1);
        assertTrue( result.get(0).get("value").equals("the value"),"update failed");
    }


}
