package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * User: Stephan Bösebeck
 * Date: 30.07.13
 * Time: 14:31
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
public class MorphiumConfigTest {


    private Logger log = LoggerFactory.getLogger(MorphiumConfigTest.class);
    public MorphiumConfigTest() {
    }

    private MorphiumConfig getConfig() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.getConnectionSettings().setDatabase("test");
        cfg.getClusterSettings()
        .setHostSeed("localhost:27017");
        cfg.getAuthSettings().setMongoAuthDb("admin")
        .setMongoLogin("login")
        .setMongoPassword("12345");
        return cfg;
    }
    @Test
    public void credentialsEncrypted() {
        MorphiumConfig def = getConfig();
        var cfg = MorphiumConfig.fromProperties(def.asProperties());
        cfg.getEncryptionSettings().setCredentialsEncrypted(true)
        .setCredentialsEncryptionKey("1234567890abcdef")
        .setCredentialsDecryptionKey("1234567890abcdef");
        var enc = new AESEncryptionProvider();
        enc.setEncryptionKey("1234567890abcdef".getBytes());
        cfg.getAuthSettings().setMongoAuthDb(Base64.getEncoder().encodeToString(enc.encrypt(cfg.getAuthSettings().getMongoAuthDb().getBytes(StandardCharsets.UTF_8))))
        .setMongoPassword(Base64.getEncoder().encodeToString(enc.encrypt(cfg.getAuthSettings().getMongoPassword().getBytes(StandardCharsets.UTF_8))))
        .setMongoLogin(Base64.getEncoder().encodeToString(enc.encrypt(cfg.getAuthSettings().getMongoLogin().getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testToString() throws Exception {
        MorphiumConfig def = getConfig();
        def.getClusterSettings().addHostToSeed("localhost:27018");
        String cfg = def.toString();
        log.info("Config: " + cfg);
        MorphiumConfig c = MorphiumConfig.createFromJson(cfg);
        log.info("Host-Seed: {}", c.getClusterSettings().getHostSeed());
        assert(!c.getClusterSettings().getHostSeed().isEmpty());
    }


    @Test
    public void testDefaultProps() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
        log.info("ReadPreference: " + cfg.getDriverSettings().getDefaultReadPreference().toString());
        Properties p = cfg.asProperties();

        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
        }

        p.store(System.out, "testproperties");
        String cfgStr = cfg.toString();
        log.info("Got: " + cfgStr);
    }

    @Test
    public void testSystemOverrides() throws Exception {
        System.getProperties().setProperty("morphium.database", "broken");
        MorphiumConfig cfg = new MorphiumConfig();
        log.info("Got db {} != {} ", cfg.getConnectionSettings().getDatabase(), System.getProperty("morphium.database"));
    }

    @Test
    public void partialJsonTest() throws Exception {
        String json = "{ \"hosts\":\"localhost:27018, localhost:27099\", \"database\" : \"testdb\", \"safe_mode\" : true , \"global_fsync\" : false , \"globalJ\" : false , \"write_timeout\" : 9990 }";
        MorphiumConfig cfg = MorphiumConfig.createFromJson(json);
        assert(cfg.getConnectionSettings().getDatabase().equals("testdb"));
        assert(cfg.getClusterSettings().getHostSeed().size() == 2);
        assert(cfg.getClusterSettings().getHostSeed().get(0).endsWith(":27018"));
        assert(cfg.getClusterSettings().getHostSeed().get(1).endsWith(":27099"));
    }


    @Test
    public void testEnum() {
        Properties p = new Properties();
        p.setProperty("morphium.indexCheck", "WARN_ON_STARTUP");
        MorphiumConfig cfg = MorphiumConfig.fromProperties("morphium", p);
        assert(cfg.getCollectionCheckSettings().getIndexCheck().equals(IndexCheck.WARN_ON_STARTUP));
        assert(cfg.getObjectMappingSettings().isAutoValues());
        assert(cfg.getClusterSettings().getHostSeed().size() == 0);
    }

    @Test
    public void testHosts() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.getClusterSettings().addHostToSeed("localhost:9999").addHostToSeed("localhost", 1000);
        assert(cfg.getClusterSettings().getHostSeed().size() == 2);
        cfg.getClusterSettings().setHostSeed("localhost:9999,localhost:2222,localhost:2344");
        assert(cfg.getClusterSettings().getHostSeed().size() == 3);
        cfg.getClusterSettings().setHostSeed("localhost,localhost,localhost,localhost", "1, 2,   3,4");
        assert(cfg.getClusterSettings().getHostSeed().size() == 4);
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
        assertEquals(1, cfg.getClusterSettings().getHostSeed().size());
        assertEquals("thingy", cfg.getConnectionSettings().getDatabase());
        assertEquals(11, cfg.getConnectionSettings().getMinConnections());
        assertEquals(120, cfg.getConnectionSettings().getMaxConnections());
    }


    @Test
    public void testMorphiumConfig() {
        MorphiumConfig cfg = getConfig();
        assertNotNull(cfg.getConnectionSettings().getDatabase());
        assertEquals(cfg.getConnectionSettings().getDatabase(), cfg.getConnectionSettings().getDatabase());
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
        assertNotNull(cfg.getConnectionSettings().getDatabase());
        assert(cfg.getConnectionSettings().getDatabase().equals(cg.getConnectionSettings().getDatabase()));
        assert(cfg.getClusterSettings().getHostSeed().size() != 0);
    }

    @Test
    public void testToPropertiesPrefix() throws Exception {
        Properties p = getConfig().asProperties("prefix");

        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
            assert(k.toString().startsWith("prefix."));
        }

        p.store(System.out, "testproperties");
        MorphiumConfig cfg = MorphiumConfig.fromProperties("prefix", p);
        assert(cfg.getConnectionSettings().getDatabase().equals(getConfig().getConnectionSettings().getDatabase()));
        assert(cfg.getClusterSettings().getHostSeed().size() != 0);
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
        assert(cfg.getClusterSettings().getHostSeed().size() == 1);
        assert(cfg.getConnectionSettings().getDatabase().equals("thingy"));
        assert(cfg.getDriverSettings().isRetryReads());
    }


}
