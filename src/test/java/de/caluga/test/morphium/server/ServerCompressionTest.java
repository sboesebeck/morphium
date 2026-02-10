package de.caluga.test.morphium.server;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumConfig.CompressionType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.server.MorphiumServer;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Tests for compression (SNAPPY, ZLIB) between morphium client and MorphiumServer.
 * Verifies that compressed wire protocol messages are correctly handled end-to-end.
 */
@Tag("server")
public class ServerCompressionTest {
    private static final Logger log = LoggerFactory.getLogger(ServerCompressionTest.class);

    private int nextPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            return 19000 + (int) (Math.random() * 1000);
        }
    }

    private void startServer(MorphiumServer srv, int port) throws Exception {
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    /**
     * Test: Client with SNAPPY compression connects to server without compression.
     * The server must correctly unwrap OpCompressed messages from the client.
     */
    @Test
    public void testClientSnappyServerNoop() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("compression_test");
            cfg.setMaxConnections(5);
            cfg.setCompressionType(CompressionType.SNAPPY);

            Morphium morphium = new Morphium(cfg);

            try (morphium) {
                // Store documents
                for (int i = 0; i < 10; i++) {
                    UncachedObject uc = new UncachedObject();
                    uc.setCounter(i);
                    uc.setStrValue("snappy-" + i);
                    morphium.store(uc);
                }
                log.info("Stored 10 documents with SNAPPY compression");

                // Read back and verify
                var results = morphium.createQueryFor(UncachedObject.class).sort("counter").asList();
                assertEquals(10, results.size(), "Should have stored 10 documents");
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, results.get(i).getCounter());
                    assertEquals("snappy-" + i, results.get(i).getStrValue());
                }
                log.info("All 10 documents verified with SNAPPY compression");
            }
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Client without compression connects to server with SNAPPY compression.
     * The server sends compressed responses; the client must decompress them.
     */
    @Test
    public void testClientNoopServerSnappy() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60, OpCompressed.COMPRESSOR_SNAPPY);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("compression_test");
            cfg.setMaxConnections(5);
            // No compression on client side

            Morphium morphium = new Morphium(cfg);

            try (morphium) {
                for (int i = 0; i < 10; i++) {
                    UncachedObject uc = new UncachedObject();
                    uc.setCounter(i);
                    uc.setStrValue("server-snappy-" + i);
                    morphium.store(uc);
                }
                log.info("Stored 10 documents (server SNAPPY, client no compression)");

                var results = morphium.createQueryFor(UncachedObject.class).sort("counter").asList();
                assertEquals(10, results.size());
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, results.get(i).getCounter());
                    assertEquals("server-snappy-" + i, results.get(i).getStrValue());
                }
                log.info("All 10 documents verified (server SNAPPY compression)");
            }
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Both client and server use SNAPPY compression.
     * Full bidirectional compression.
     */
    @Test
    public void testBothSnappy() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60, OpCompressed.COMPRESSOR_SNAPPY);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("compression_test");
            cfg.setMaxConnections(5);
            cfg.setCompressionType(CompressionType.SNAPPY);

            Morphium morphium = new Morphium(cfg);

            try (morphium) {
                // CRUD operations
                for (int i = 0; i < 20; i++) {
                    UncachedObject uc = new UncachedObject();
                    uc.setCounter(i);
                    uc.setStrValue("both-snappy-" + i);
                    morphium.store(uc);
                }
                log.info("Stored 20 documents with bidirectional SNAPPY");

                // Read
                var results = morphium.createQueryFor(UncachedObject.class).sort("counter").asList();
                assertEquals(20, results.size());

                // Query with filter
                var filtered = morphium.createQueryFor(UncachedObject.class)
                        .f("counter").gt(9)
                        .sort("counter")
                        .asList();
                assertEquals(10, filtered.size());
                assertEquals(10, filtered.get(0).getCounter());

                // Count
                long count = morphium.createQueryFor(UncachedObject.class).countAll();
                assertEquals(20, count);

                // Update
                var toUpdate = morphium.createQueryFor(UncachedObject.class)
                        .f("counter").eq(5)
                        .get();
                assertNotNull(toUpdate);
                toUpdate.setStrValue("updated-snappy");
                morphium.store(toUpdate);

                var updated = morphium.createQueryFor(UncachedObject.class)
                        .f("counter").eq(5)
                        .get();
                assertEquals("updated-snappy", updated.getStrValue());

                // Delete
                morphium.delete(morphium.createQueryFor(UncachedObject.class).f("counter").gt(14));
                long remaining = morphium.createQueryFor(UncachedObject.class).countAll();
                assertEquals(15, remaining);

                log.info("All CRUD operations verified with bidirectional SNAPPY compression");
            }
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Both client and server use ZLIB compression.
     */
    @Test
    public void testBothZlib() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60, OpCompressed.COMPRESSOR_ZLIB);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("compression_test");
            cfg.setMaxConnections(5);
            cfg.setCompressionType(CompressionType.ZLIB);

            Morphium morphium = new Morphium(cfg);

            try (morphium) {
                for (int i = 0; i < 10; i++) {
                    UncachedObject uc = new UncachedObject();
                    uc.setCounter(i);
                    uc.setStrValue("zlib-" + i);
                    morphium.store(uc);
                }

                var results = morphium.createQueryFor(UncachedObject.class).sort("counter").asList();
                assertEquals(10, results.size());
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, results.get(i).getCounter());
                    assertEquals("zlib-" + i, results.get(i).getStrValue());
                }
                log.info("All 10 documents verified with bidirectional ZLIB compression");
            }
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Client SNAPPY, server ZLIB (mixed compression).
     * Client sends SNAPPY-compressed, server sends ZLIB-compressed.
     */
    @Test
    public void testClientSnappyServerZlib() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60, OpCompressed.COMPRESSOR_ZLIB);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("compression_test");
            cfg.setMaxConnections(5);
            cfg.setCompressionType(CompressionType.SNAPPY);

            Morphium morphium = new Morphium(cfg);

            try (morphium) {
                for (int i = 0; i < 10; i++) {
                    UncachedObject uc = new UncachedObject();
                    uc.setCounter(i);
                    uc.setStrValue("mixed-" + i);
                    morphium.store(uc);
                }

                var results = morphium.createQueryFor(UncachedObject.class).sort("counter").asList();
                assertEquals(10, results.size());
                log.info("Mixed compression (client SNAPPY, server ZLIB) works correctly");
            }
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Direct driver-level test with SNAPPY compression.
     * Uses SingleMongoConnectDriver to verify low-level compression works.
     */
    @Test
    public void testDriverLevelSnappy() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60);
        startServer(srv, port);

        try {
            SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
            drv.setHostSeed("localhost:" + port);
            drv.setMaxConnections(5);
            drv.setHeartbeatFrequency(500);
            drv.setMaxWaitTime(5000);
            drv.setCompression(CompressionType.SNAPPY.getCode());

            drv.connect();
            log.info("Driver connected with SNAPPY compression");

            // Execute a ping command
            var con = drv.getPrimaryConnection(null);
            assertNotNull(con, "Should get a primary connection");

            var pingCmd = new de.caluga.morphium.driver.commands.GenericCommand(con);
            pingCmd.setDb("admin");
            pingCmd.setColl("");
            pingCmd.setCmdData(Doc.of("ping", 1));
            int msgId = con.sendCommand(pingCmd);
            var pingResult = con.readSingleAnswer(msgId);
            assertNotNull(pingResult);
            assertEquals(1.0, pingResult.get("ok"));
            log.info("Ping successful with SNAPPY compression");

            drv.releaseConnection(con);
            drv.close();
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Verify hello response advertises compression when server is configured for it.
     */
    @Test
    public void testHelloResponseAdvertisesCompression() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60, OpCompressed.COMPRESSOR_SNAPPY);
        startServer(srv, port);

        try {
            SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
            drv.setHostSeed("localhost:" + port);
            drv.setMaxConnections(5);
            drv.setHeartbeatFrequency(500);
            drv.setMaxWaitTime(5000);
            // No compression on client to get clean hello

            drv.connect();

            var con = drv.getPrimaryConnection(null);
            assertNotNull(con);

            // Send hello command
            var helloCmd = new de.caluga.morphium.driver.commands.HelloCommand(con);
            helloCmd.setDb("admin");
            var helloResult = helloCmd.execute();
            assertNotNull(helloResult);
            assertTrue(helloResult.isOk());

            // Check compression advertisement
            List<String> compression = helloResult.getCompression();
            assertNotNull(compression, "Hello response should include compression");
            assertTrue(compression.contains("snappy"), "Should advertise snappy");
            log.info("Hello response correctly advertises compression: {}", compression);

            drv.releaseConnection(con);
            drv.close();
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Multiple concurrent connections with SNAPPY compression.
     * Ensures compression works under parallel load.
     */
    @Test
    public void testConcurrentSnappyConnections() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 100, 60, OpCompressed.COMPRESSOR_SNAPPY);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("concurrent_compression");
            cfg.setMaxConnections(10);
            cfg.setCompressionType(CompressionType.SNAPPY);

            Morphium morphium = new Morphium(cfg);
            AtomicInteger errors = new AtomicInteger(0);
            AtomicInteger completed = new AtomicInteger(0);
            int threadCount = 5;
            int docsPerThread = 10;

            Thread[] threads = new Thread[threadCount];
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        for (int i = 0; i < docsPerThread; i++) {
                            UncachedObject uc = new UncachedObject();
                            uc.setCounter(threadId * 1000 + i);
                            uc.setStrValue("thread-" + threadId + "-doc-" + i);
                            morphium.store(uc);
                        }
                        completed.incrementAndGet();
                    } catch (Exception e) {
                        log.error("Thread {} failed: {}", threadId, e.getMessage(), e);
                        errors.incrementAndGet();
                    }
                });
                threads[t].start();
            }

            for (Thread thread : threads) {
                thread.join(30000);
            }

            assertEquals(0, errors.get(), "No threads should have errors");
            assertEquals(threadCount, completed.get(), "All threads should complete");

            long count = morphium.createQueryFor(UncachedObject.class).countAll();
            assertEquals(threadCount * docsPerThread, count,
                    "Should have stored " + (threadCount * docsPerThread) + " documents");
            log.info("Concurrent SNAPPY compression test passed: {} documents stored", count);

            morphium.close();
        } finally {
            srv.shutdown();
        }
    }

    /**
     * Test: Large documents with SNAPPY compression.
     * Ensures compression works for larger payloads.
     */
    @Test
    public void testLargeDocumentSnappy() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 60, OpCompressed.COMPRESSOR_SNAPPY);
        startServer(srv, port);

        try {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.setHostSeed("localhost:" + port);
            cfg.setDatabase("large_doc_test");
            cfg.setMaxConnections(5);
            cfg.setCompressionType(CompressionType.SNAPPY);

            Morphium morphium = new Morphium(cfg);

            try (morphium) {
                // Create a document with a large string value
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    sb.append("This is a large document for compression testing. Line " + i + ". ");
                }
                String largeValue = sb.toString();

                UncachedObject uc = new UncachedObject();
                uc.setCounter(42);
                uc.setStrValue(largeValue);
                morphium.store(uc);

                var result = morphium.createQueryFor(UncachedObject.class)
                        .f("counter").eq(42)
                        .get();
                assertNotNull(result);
                assertEquals(42, result.getCounter());
                assertEquals(largeValue, result.getStrValue());
                log.info("Large document test passed. Document size: ~{} bytes", largeValue.length());
            }
        } finally {
            srv.shutdown();
        }
    }
}
