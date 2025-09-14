package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.encryption.AESEncryptionProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 30.07.13
 * Time: 14:31
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class MorphiumConfigTest {


    private Logger log = LoggerFactory.getLogger(MorphiumConfigTest.class);
    public MorphiumConfigTest() {
    }


    @Test
    public void testCopySettings() {

        var cfg = getConfig();
        var copy = cfg.messagingSettings().copy();
        assertFalse(cfg.messagingSettings() == copy);
        assertEquals(cfg.messagingSettings(), copy);


        var fullCopy = cfg.createCopy();
        assertEquals(fullCopy, cfg);
        assertFalse(fullCopy == cfg);

        log.info("cfg.hash={} copy.hash={}", cfg.hashCode(), fullCopy.hashCode());

        fullCopy.messagingSettings().setMessagingImplementation("Test");
        log.info("cfg.hash={} copy.hash={}", cfg.hashCode(), fullCopy.hashCode());


    }
    private MorphiumConfig getConfig() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("test");
        cfg.clusterSettings()
           .setHostSeed("localhost:27017");
        cfg.authSettings().setMongoAuthDb("admin")
           .setMongoLogin("login")
           .setMongoPassword("12345");
        return cfg;
    }
    @Test
    public void credentialsEncrypted() {
        MorphiumConfig def = getConfig();
        var cfg = MorphiumConfig.fromProperties(def.asProperties());
        cfg.encryptionSettings().setCredentialsEncrypted(true)
           .setCredentialsEncryptionKey("1234567890abcdef")
           .setCredentialsDecryptionKey("1234567890abcdef");
        var enc = new AESEncryptionProvider();
        enc.setEncryptionKey("1234567890abcdef".getBytes());
        cfg.authSettings().setMongoAuthDb(Base64.getEncoder().encodeToString(enc.encrypt(cfg.authSettings().getMongoAuthDb().getBytes(StandardCharsets.UTF_8))))
           .setMongoPassword(Base64.getEncoder().encodeToString(enc.encrypt(cfg.authSettings().getMongoPassword().getBytes(StandardCharsets.UTF_8))))
           .setMongoLogin(Base64.getEncoder().encodeToString(enc.encrypt(cfg.authSettings().getMongoLogin().getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testToString() throws Exception {
        MorphiumConfig def = getConfig();
        def.clusterSettings().addHostToSeed("localhost:27018");
        String cfg = def.toString();
        log.info("Config: " + cfg);
        MorphiumConfig c = MorphiumConfig.createFromJson(cfg);
        log.info("Host-Seed: {}", c.clusterSettings().getHostSeed());
        assertFalse(c.clusterSettings().getHostSeed().isEmpty());
        log.info("c.toString(): {}", c.toString());
        log.info("cfg string:   {}", cfg);
        assertEquals(c.toString(), cfg);
    }


    @Test
    public void testDefaultProps() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
        log.info("ReadPreference: " + cfg.driverSettings().getDefaultReadPreference().toString());
        Properties p = cfg.asProperties();

        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
        }

        p.store(System.out, "testproperties");
        String cfgStr = cfg.toString();
        log.info("Got: " + cfgStr);
    }


    @Test
    public void partialJsonTest() throws Exception {
        String json = "{ \"hosts\":\"localhost:27018, localhost:27099\", \"database\" : \"testdb\", \"safe_mode\" : true , \"global_fsync\" : false , \"globalJ\" : false , \"write_timeout\" : 9990 }";
        MorphiumConfig cfg = MorphiumConfig.createFromJson(json);
        assertEquals(cfg.connectionSettings().getDatabase(), "testdb");
        assertEquals(cfg.clusterSettings().getHostSeed().size(), 2);
        assertTrue(cfg.clusterSettings().getHostSeed().get(0).endsWith(":27018"));
        assertTrue(cfg.clusterSettings().getHostSeed().get(1).endsWith(":27099"));
    }


    @Test
    public void testEnum() {
        Properties p = new Properties();
        p.setProperty("morphium.indexCheck", "WARN_ON_STARTUP");
        MorphiumConfig cfg = MorphiumConfig.fromProperties("morphium", p);
        assertEquals(cfg.collectionCheckSettings().getIndexCheck(), IndexCheck.WARN_ON_STARTUP);
        assertTrue(cfg.objectMappingSettings().isAutoValues());
        assertEquals(cfg.clusterSettings().getHostSeed().size(), 0);
    }

    @Test
    public void testHosts() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().addHostToSeed("localhost:9999").addHostToSeed("localhost", 1000);
        assertEquals(cfg.clusterSettings().getHostSeed().size(), 2);
        cfg.clusterSettings().setHostSeed("localhost:9999,localhost:2222,localhost:2344");
        assertEquals(cfg.clusterSettings().getHostSeed().size(), 3);
        cfg.clusterSettings().setHostSeed("localhost,localhost,localhost,localhost", "1, 2,   3,4");
        assertEquals(cfg.clusterSettings().getHostSeed().size(), 4);
    }


    @Test
    public void partialPropsTest() {
        Properties p = new Properties();
        p.put("maximumRetriesAsyncWriter", "10");
        p.put("socketTimeout", "1000");
        p.put("database", "thingy");
        p.put("hosts", "localhost:27017");
        p.put("maxConnections", "120");
        p.put("minConnections", "11");
        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assertEquals(1, cfg.clusterSettings().getHostSeed().size());
        assertEquals("thingy", cfg.connectionSettings().getDatabase());
        assertEquals(11, cfg.connectionSettings().getMinConnections());
        assertEquals(120, cfg.connectionSettings().getMaxConnections());
    }


    @Test
    public void testMorphiumConfig() {
        MorphiumConfig cfg = getConfig();
        assertNotNull(cfg.connectionSettings().getDatabase());
        assertEquals(cfg.connectionSettings().getDatabase(), cfg.connectionSettings().getDatabase());
    }

    @Test
    public void testToProperties() throws Exception {
        var cg = getConfig();
        Properties p = cg.asProperties();

        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
        }

        p.store(System.out, "testproperties");
        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assertNotNull(cfg.connectionSettings().getDatabase());
        assertEquals(cfg.connectionSettings().getDatabase(), cg.connectionSettings().getDatabase());
        assertFalse(cfg.clusterSettings().getHostSeed().isEmpty() );
    }

    @Test
    public void testToPropertiesPrefix() throws Exception {
        Properties p = getConfig().asProperties("prefix");

        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
            assertTrue(k.toString().startsWith("prefix."));
        }

        p.store(System.out, "testproperties");
        MorphiumConfig cfg = MorphiumConfig.fromProperties("prefix", p);
        assertEquals(cfg.connectionSettings().getDatabase(), getConfig().connectionSettings().getDatabase());
        assertTrue(cfg.clusterSettings().getHostSeed().size() != 0);
    }

    @Test
    public void testReadWithPrefix() {
        Properties p = new Properties();
        p.put("prefix.maximumRetriesAsyncWriter", "10");
        p.put("prefix.socketTimeout", "1000");
        p.put("prefix.hosts", "localhost:27017");
        p.put("prefix.database", "thingy");
        p.put("prefix.retryReads", "true");
        MorphiumConfig cfg = MorphiumConfig.fromProperties("prefix", p);
        assertEquals(cfg.clusterSettings().getHostSeed().size(), 1);
        assertEquals(cfg.connectionSettings().getDatabase(), "thingy");
        assertTrue(cfg.driverSettings().isRetryReads());
    }


}
