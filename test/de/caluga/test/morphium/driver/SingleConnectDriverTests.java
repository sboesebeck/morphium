package de.caluga.test.morphium.driver;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.commands.StepDownCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleConnectDriverTests extends DriverTestBase {
    private Logger log= LoggerFactory.getLogger(SingleConnectDriverTests.class);

    @Test
    public void hostSeedTest() throws Exception{
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed("localhost:27017");
        drv.setConnectionTimeout(1000);
        drv.setHeartbeatFrequency(1000);
        drv.setMaxWaitTime(1000);
        drv.connect();

        Thread.sleep(1000);
        assertThat(drv.getHostSeed().size()).isGreaterThan(1); //should update hostseed
        for(String n:drv.getHostSeed()){
            log.info("HostSeed: "+n);
        }
        drv.close();
    }


    @Test
    public void testHeartbeat() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        log.info("Hearbeat frequency " + drv.getHeartbeatFrequency());
        Thread.sleep(500);
        var h = drv.getConnection().getConnectedTo();
        StepDownCommand cmd = new StepDownCommand(drv.getConnection()).setTimeToStepDown(10).setForce(Boolean.TRUE);
        var res = cmd.execute();
        //log.info("result: " + Utils.toJsonString(res));
        while (true) {
            while (!drv.isConnected() || drv.getConnection().getConnectedTo() == null || drv.getConnection().getConnectedTo().equals(h)) {
                log.info("Waiting for failover...");
                Thread.sleep(1500);
            }
            String hst = drv.getConnection().getConnectedTo();
            log.info("Failover...to " + drv.getConnection().getConnectedTo());
            boolean br = true;
            for (int i = 0; i < 2; i++) {
                Thread.sleep(1000);
                if (drv.getConnection().getConnectedTo() == null || !drv.getConnection().getConnectedTo().equals(hst)) {
                    log.info("Connection did not stick...");
                    br = false;
                    break;
                }
            }
            if (br)
                break;
        }
        assertThat(drv.isConnected());
        assertThat(drv.getConnection().getConnectedTo()).isNotEqualTo(h);
        assertThat(h).isNotNull();
        assertThat(drv.getConnection().getConnectedTo()).isNotNull();
        log.info("Connection changed from " + h + " to " + drv.getConnection().getConnectedTo());
        log.info("HostSeed:");
        for (var c : drv.getHostSeed()) {
            log.info("---> " + c);
        }
        drv.close();


    }


    @Test
    public void testFailover() throws Exception {
        SingleMongoConnectDriver drv = getDriver();
        log.info("Hearbeat frequency " + drv.getHeartbeatFrequency());
        Thread.sleep(1500);
        var h = drv.getConnection().getConnectedTo();
        ShutdownCommand cmd = new ShutdownCommand(drv.getConnection()).setForce(Boolean.TRUE).setTimeoutSecs(2);
        var res = cmd.execute();
        log.info("result: " + Utils.toJsonString(res));

        while (true) {
            while (!drv.isConnected() || drv.getConnection().getConnectedTo() == null || drv.getConnection().getConnectedTo().equals(h)) {
                log.info("Waiting for failover...still connected to " + h + " == " + drv.getConnection().getConnectedTo());
                Thread.sleep(1500);
            }
            String hst = drv.getConnection().getConnectedTo();
            log.info("Failover...to " + drv.getConnection().getConnectedTo());
            boolean br = true;
            for (int i = 0; i < 2; i++) {
                Thread.sleep(1000);
                if (drv.getConnection().getConnectedTo() == null || !drv.getConnection().getConnectedTo().equals(hst)) {
                    log.info("Connection did not stick...");
                    br = false;
                    break;
                }
            }
            if (br)
                break;
        }
        assertThat(drv.isConnected());
        assertThat(drv.getConnection().getConnectedTo()).isNotEqualTo(h);
        assertThat(h).isNotNull();
        assertThat(drv.getConnection().getConnectedTo()).isNotNull();
        log.info("Connection changed from " + h + " to " + drv.getConnection().getConnectedTo());
        log.info("HostSeed:");
        for (var c : drv.getHostSeed()) {
            log.info("---> " + c);
        }
        drv.close();


    }


}
