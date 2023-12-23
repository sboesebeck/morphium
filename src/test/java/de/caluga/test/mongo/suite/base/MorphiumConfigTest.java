package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * User: Stephan Bösebeck
 * Date: 30.07.13
 * Time: 14:31
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
public class MorphiumConfigTest extends MorphiumTestBase {

    @Test
    public void credentialsEncrypted() {
        var cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.setCredentialsEncrypted(true);
        cfg.setCredentialsEncryptionKey("1234567890abcdef");
        cfg.setCredentialsDecryptionKey("1234567890abcdef");
        var enc = new AESEncryptionProvider();
        enc.setEncryptionKey("1234567890abcdef".getBytes());
        cfg.setMongoAuthDb(Base64.getEncoder().encodeToString(enc.encrypt(cfg.getMongoAuthDb().getBytes(StandardCharsets.UTF_8))));
        cfg.setMongoPassword(Base64.getEncoder().encodeToString(enc.encrypt(cfg.getMongoPassword().getBytes(StandardCharsets.UTF_8))));
        cfg.setMongoLogin(Base64.getEncoder().encodeToString(enc.encrypt(cfg.getMongoLogin().getBytes(StandardCharsets.UTF_8))));

        var m = new Morphium(cfg);
        assertNotNull(m);
        m.close();
    }

    @Test
    public void testToString() throws Exception {
        String cfg = morphium.getConfig().toString();
        log.info("Config: " + cfg);

        MorphiumConfig c = MorphiumConfig.createFromJson(cfg);
        assert (c.getHostSeed().size() >= 1);
    }

    @Test
    public void testDefaultProps() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
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
        assert (cfg.getDatabase().equals("testdb"));
        assert (cfg.getHostSeed().size() == 2);
        assert (cfg.getHostSeed().get(0).endsWith(":27018"));
        assert (cfg.getHostSeed().get(1).endsWith(":27099"));
    }


    @Test
    public void testEnum() {
        Properties p = new Properties();
        p.setProperty("morphium.indexCheck", "WARN_ON_STARTUP");
        MorphiumConfig cfg = MorphiumConfig.fromProperties("morphium", p);
        assert (cfg.getIndexCheck().equals(MorphiumConfig.IndexCheck.WARN_ON_STARTUP));
        assert (cfg.isAutoValuesEnabled());
        assert (cfg.getHostSeed().size() == 0);
    }

    @Test
    public void testHosts() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.addHostToSeed("localhost:9999");
        cfg.addHostToSeed("localhost", 1000);
        assert (cfg.getHostSeed().size() == 2);

        cfg.setHostSeed("localhost:9999,localhost:2222,localhost:2344");
        assert (cfg.getHostSeed().size() == 3);

        cfg.setHostSeed("localhost,localhost,localhost,localhost", "1, 2,   3,4");
        assert (cfg.getHostSeed().size() == 4);

    }


    @Test
    public void partialPropsTest() {
        Properties p = new Properties();
        p.put("maximumRetriesAsyncWriter", "10");
        p.put("socketTimeout", "1000");
        p.put("database", "thingy");
        p.put("hosts", "localhost:27017");

        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assert (cfg.getHostSeed().size() == 1);
        assert (cfg.getDatabase().equals("thingy"));
    }

    @Test
    public void testToProperties() throws Exception {
        Properties p = morphium.getConfig().asProperties();
        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
        }
        p.store(System.out, "testproperties");

        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assert (cfg.getDatabase().equals(morphium.getConfig().getDatabase()));
        assert (cfg.getHostSeed().size() != 0);
    }

    @Test
    public void testToPropertiesPrefix() throws Exception {
        Properties p = morphium.getConfig().asProperties("prefix");

        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
            assert (k.toString().startsWith("prefix."));
        }
        p.store(System.out, "testproperties");

        MorphiumConfig cfg = MorphiumConfig.fromProperties("prefix", p);
        assert (cfg.getDatabase().equals(morphium.getConfig().getDatabase()));
        assert (cfg.getHostSeed().size() != 0);
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
        assert (cfg.getHostSeed().size() == 1);
        assert (cfg.getDatabase().equals("thingy"));
        assert (cfg.isRetryReads());
    }


}
