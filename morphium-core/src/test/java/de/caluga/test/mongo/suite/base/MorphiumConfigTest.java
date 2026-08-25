package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import de.caluga.morphium.encryption.AESEncryptionProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 30.07.13
 * Time: 14:31
 * <p>
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
    public void useBsonDateForJavaTime_survivesPropertiesRoundTrip() {
        // Settings.asProperties() is reflection-based, so a new field should carry automatically --
        // but a silently dropped setting is invisible until someone's config stops taking effect,
        // so pin it rather than trust the mechanism.
        MorphiumConfig def = getConfig();
        assertFalse(def.objectMappingSettings().isUseBsonDateForJavaTime(),
            "default must stay false -- this is an opt-in flag");

        def.objectMappingSettings().setUseBsonDateForJavaTime(true);
        Properties props = def.asProperties();
        // MorphiumConfig.asProperties() merges every Settings object into one flat namespace
        // (MorphiumConfig.java:1329), so the key is the bare field name -- no settings prefix.
        assertEquals("true", props.getProperty("useBsonDateForJavaTime"),
            "the flag must appear in asProperties()");

        MorphiumConfig restored = MorphiumConfig.fromProperties(props);
        assertTrue(restored.objectMappingSettings().isUseBsonDateForJavaTime(),
            "the flag must survive an asProperties()/fromProperties() round trip");

        // and the other direction, so a false value is not just the default leaking through
        def.objectMappingSettings().setUseBsonDateForJavaTime(false);
        assertFalse(MorphiumConfig.fromProperties(def.asProperties())
            .objectMappingSettings().isUseBsonDateForJavaTime());
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


    /**
     * A read preference set on the config must survive an asProperties()/fromProperties()
     * round trip. It used to be dropped silently, so a config that was persisted and reloaded
     * fell back to the class default (nearest) and started reading from secondaries — which
     * breaks read-after-write on a replica set without any visible error.
     */
    @Test
    public void readPreferenceSurvivesPropertiesRoundTrip() {
        MorphiumConfig cfg = getConfig();
        cfg.driverSettings().setDefaultReadPreference(ReadPreference.primary());

        MorphiumConfig reloaded = MorphiumConfig.fromProperties(cfg.asProperties());

        assertNotNull(reloaded.driverSettings().getDefaultReadPreference());
        assertEquals(ReadPreferenceType.PRIMARY,
                     reloaded.driverSettings().getDefaultReadPreference().getType());
    }

    /** Same for a prefixed round trip, which uses a different key derivation. */
    @Test
    public void readPreferenceSurvivesPrefixedPropertiesRoundTrip() {
        MorphiumConfig cfg = getConfig();
        cfg.driverSettings().setDefaultReadPreference(ReadPreference.secondaryPreferred());

        MorphiumConfig reloaded = MorphiumConfig.fromProperties("prefix", cfg.asProperties("prefix"));

        assertNotNull(reloaded.driverSettings().getDefaultReadPreference());
        assertEquals(ReadPreferenceType.SECONDARY_PREFERRED,
                     reloaded.driverSettings().getDefaultReadPreference().getType());
    }

    /**
     * createCopy() goes through Settings.copy(), which drops transient fields — the copy used to
     * end up with a type name saying PRIMARY and a preference object saying NEAREST. Messaging
     * tests build their extra Morphium instances from such copies.
     */
    @Test
    public void readPreferenceSurvivesCreateCopy() {
        MorphiumConfig cfg = getConfig();
        cfg.driverSettings().setDefaultReadPreference(ReadPreference.primary());

        MorphiumConfig copy = cfg.createCopy();

        assertNotNull(copy.driverSettings().getDefaultReadPreference());
        assertEquals(ReadPreferenceType.PRIMARY,
                     copy.driverSettings().getDefaultReadPreference().getType());
        assertEquals("PRIMARY", copy.driverSettings().getDefaultReadPreferenceType());
    }

    /** An unparseable type name must not overwrite a valid setting, nor be stored and re-emitted. */
    @Test
    public void invalidReadPreferenceTypeKeepsPreviousSetting() {
        MorphiumConfig cfg = getConfig();
        cfg.driverSettings().setDefaultReadPreference(ReadPreference.primary());

        cfg.driverSettings().setDefaultReadPreferenceType("bogusValue");

        assertEquals(ReadPreferenceType.PRIMARY,
                     cfg.driverSettings().getDefaultReadPreference().getType());
        assertEquals("PRIMARY", cfg.driverSettings().getDefaultReadPreferenceType());
    }

    /** A tagged preference keeps its tags as long as it is not round-tripped through properties. */
    @Test
    public void tagSetSurvivesInMemorySetting() {
        MorphiumConfig cfg = getConfig();
        ReadPreference tagged = ReadPreference.secondary();
        tagged.setTagSet(Map.of("dc", "eu-west"));
        cfg.driverSettings().setDefaultReadPreference(tagged);

        assertEquals(ReadPreferenceType.SECONDARY,
                     cfg.driverSettings().getDefaultReadPreference().getType());
        assertNotNull(cfg.driverSettings().getDefaultReadPreference().getTagSet());
        assertEquals("eu-west", cfg.driverSettings().getDefaultReadPreference().getTagSet().get("dc"));
    }

    /** A config that never touched the read preference keeps the class default. */
    @Test
    public void defaultReadPreferenceIsKeptWhenNeverSet() {
        MorphiumConfig reloaded = MorphiumConfig.fromProperties(getConfig().asProperties());

        assertNotNull(reloaded.driverSettings().getDefaultReadPreference());
        assertEquals(new MorphiumConfig().driverSettings().getDefaultReadPreference().getType(),
                     reloaded.driverSettings().getDefaultReadPreference().getType());
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

    // ------------------------------------------------------------------
    // setAutoIndexAndCappedCreationOnWrite – must set BOTH checks
    // ------------------------------------------------------------------

    @Test
    public void autoIndexAndCapped_enabledSetsBothChecks() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setAutoIndexAndCappedCreationOnWrite(true);

        assertEquals(IndexCheck.CREATE_ON_WRITE_NEW_COL,
                cfg.collectionCheckSettings().getIndexCheck(),
                "IndexCheck should be CREATE_ON_WRITE_NEW_COL when enabled");
        assertEquals(CappedCheck.CREATE_ON_WRITE_NEW_COL,
                cfg.collectionCheckSettings().getCappedCheck(),
                "CappedCheck should be CREATE_ON_WRITE_NEW_COL when enabled");
    }

    @Test
    public void autoIndexAndCapped_disabledSetsBothToNoCheck() {
        MorphiumConfig cfg = new MorphiumConfig();
        // First enable, then disable to verify it resets both
        cfg.setAutoIndexAndCappedCreationOnWrite(true);
        cfg.setAutoIndexAndCappedCreationOnWrite(false);

        assertEquals(IndexCheck.NO_CHECK,
                cfg.collectionCheckSettings().getIndexCheck(),
                "IndexCheck should be NO_CHECK when disabled");
        assertEquals(CappedCheck.NO_CHECK,
                cfg.collectionCheckSettings().getCappedCheck(),
                "CappedCheck should be NO_CHECK when disabled");
    }

    @Test
    public void autoIndexAndCapped_isAutoReflectsBothChecks() {
        MorphiumConfig cfg = new MorphiumConfig();
        assertFalse(cfg.isAutoIndexAndCappedCreationOnWrite(),
                "Should be false by default");

        cfg.setAutoIndexAndCappedCreationOnWrite(true);
        assertTrue(cfg.isAutoIndexAndCappedCreationOnWrite(),
                "Should be true after enabling");

        cfg.setAutoIndexAndCappedCreationOnWrite(false);
        assertFalse(cfg.isAutoIndexAndCappedCreationOnWrite(),
                "Should be false after disabling");
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
