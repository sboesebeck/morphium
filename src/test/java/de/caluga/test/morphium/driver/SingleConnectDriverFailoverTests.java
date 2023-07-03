
package de.caluga.test.morphium.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ReplicastStatusCommand;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.commands.StepDownCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;


public class SingleConnectDriverFailoverTests extends DriverTestBase{
    private Logger log=LoggerFactory.getLogger(SingleConnectDriverFailoverTests.class);
    @Test
    public void testFailover() throws Exception {
        // log.info("Not testing failover!");
        // if (true) return;
        SingleMongoConnectDriver drv = getDriver();
        log.info("Hearbeat frequency " + drv.getHeartbeatFrequency());

        while (!drv.isConnected()) {
            Thread.yield();
        }

        MongoConnection con = drv.getConnection();
        var originalConnectedTo = con.getConnectedTo();
        ShutdownCommand cmd = new ShutdownCommand(con).setForce(Boolean.TRUE).setTimeoutSecs(2);
        var res = cmd.executeAsync();
        //con.readNextMessage(1000);
        log.info("Shutdown command sent..." + res);
        drv.releaseConnection(con);
        Thread.sleep(5000); //shutdown takes a while

        while (true) {
            while (!drv.isConnected()) {
                Thread.sleep(500);
                log.info("... not connected");
            }

            con = drv.getConnection();

            if (con.getConnectedTo() == null || con.getConnectedTo().equals(originalConnectedTo)) {
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
        log.info("HostSeed:");

        for (var c : drv.getHostSeed()) {
            log.info("---> " + c);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("---->>> restart mongod on " + originalConnectedTo + " and press ENTER");
        br.readLine();
        log.info("Node should be started up... checking cluster");
OUT:

        while (true) {
            Thread.sleep(1000);
            con = drv.getConnection();
            ReplicastStatusCommand rstat = new ReplicastStatusCommand(con);
            var status = rstat.execute();
            drv.releaseConnection(con);
            var members = ((List<Map<String, Object>>) status.get("members"));

            for (Map<String, Object> mem : members) {
                if (mem.get("name").equals(originalConnectedTo)) {
                    Object stateStr = mem.get("stateStr");
                    log.info("Status of original host: " + stateStr);

                    if (stateStr.equals("PRIMARY")) {
                        log.info("Finally...");
                        break OUT;
                    } else if (stateStr.equals("STARTUP") || stateStr.equals("STARTUP2") || stateStr.equals("SECONDARY")) {
                        log.info("Status is still " + stateStr);
                    } else {
                        log.warn("Unknown status " + stateStr);
                    }
                }
            }
        }

        log.info("State recovered");
    }


}
