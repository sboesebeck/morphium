package de.caluga.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ReplicastStatusCommand;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.server.MorphiumServer;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.UncachedObject;

public class FailoverTests {

    private Logger log = LoggerFactory.getLogger(FailoverTests.class);
    @Test
    public void pooledDriverPrimaryChangeTest() throws Exception {
        MorphiumServer srv1 = new MorphiumServer(16016, "127.0.0.1", 1000, 10);
        MorphiumServer srv2 = new MorphiumServer(16017, "127.0.0.1", 1000, 10);
        MorphiumServer srv3 = new MorphiumServer(16018, "127.0.0.1", 1000, 10);
        var servers = List.of(srv1, srv2, srv3);
        try {
            for (var srv : servers) {
                srv.configureReplicaSet("rs_test", List.of("127.0.0.1:16016", "127.0.0.1:16017", "127.0.0.1:16018"), null, true, null);
            }

            for (var srv : servers) {
                srv.start();
            }

            log.info("Waiting for initial primary");
            AtomicReference<MorphiumServer> primary = new AtomicReference<>();
            TestUtils.waitForConditionToBecomeTrue(15000, "No primary elected", () -> {
                for (var srv : servers) {
                    if (srv.isPrimary()) {
                        primary.set(srv);
                        return true;
                    }
                }
                return false;
            });
            log.info("Initial primary is: " + primary.get().getPort());

            MorphiumConfig cfg = new MorphiumConfig();
            cfg.connectionSettings().setDatabase("failover_tests");
            cfg.driverSettings().setDriverName(PooledDriver.driverName);
            cfg.clusterSettings().addHostToSeed("127.0.0.1", 16016);
            cfg.clusterSettings().addHostToSeed("127.0.0.1", 16017);
            cfg.clusterSettings().addHostToSeed("127.0.0.1", 16018);
            cfg.clusterSettings().setHeartbeatFrequency(100);
            cfg.clusterSettings().setReplicaset(true);
            cfg.clusterSettings().setRequiredReplicaSetName("rs_test");
            cfg.collectionCheckSettings().setCappedCheck(CappedCheck.NO_CHECK);
            cfg.collectionCheckSettings().setIndexCheck(IndexCheck.NO_CHECK);
            Morphium m = new Morphium(cfg);
            Thread.sleep(100);

            log.info("Connection established!");
            AtomicInteger errors = new AtomicInteger(0);
            AtomicBoolean running = new AtomicBoolean(true);
            Thread.ofVirtual().start(() -> {
                int cnt = 0;
                while (running.get()) {
                    cnt++;
                    try {
                        m.store(new UncachedObject("test", cnt));
                        Thread.sleep(250);
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            });
            var stats = m.getDbStats("failover_tests");
            log.info("Stats: {}", stats);

            Thread.sleep(1000);
            log.info("Errors now: {}", errors.get());
            int err = errors.get();

            log.info("Shutting down primary: " + primary.get().getPort());
            primary.get().shutdown(); //forcing failover

            log.info("Waiting for new primary to be elected");
            AtomicReference<MorphiumServer> newPrimary = new AtomicReference<>();
            var remainingServers = servers.stream().filter(s -> s != primary.get()).toList();
            TestUtils.waitForConditionToBecomeTrue(15000, "No new primary elected after failover", () -> {
                for (var srv : remainingServers) {
                    if (srv.isPrimary()) {
                        newPrimary.set(srv);
                        return true;
                    }
                }
                return false;
            });

            log.info("New primary is: " + newPrimary.get().getPort());
            log.info("Failover finished - {} errors", errors.get() - err);
            err = errors.get();
            Thread.sleep(1000);
            assertTrue(err >= errors.get()); //no additional errors.

            m.close();
        } finally {
            for (var srv : servers) {
                if (srv.isRunning()) {
                    srv.shutdown();
                }
            }
        }
    }

}
