package de.caluga.morphium;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #357: a {@code mongodb://} connection string in {@code atlas-url} used to be discarded with a
 * warning, so startup failed with "no server address specified". It is now parsed - hosts
 * literally (no SRV lookup), userinfo/options/db as defaults so explicit config always wins.
 * Exercises the package-private {@link Morphium#applyStandardConnectionUri}.
 */
@Tag("core")
public class StandardConnectionStringTest {

    private static List<String> seed(MorphiumConfig cfg) {
        return cfg.clusterSettings().getHostSeed();
    }

    @Test
    void singleHostWithPortPopulatesTheSeed() {
        MorphiumConfig cfg = new MorphiumConfig();
        Morphium.applyStandardConnectionUri(cfg, "mongodb://db.example.com:27018/mydb");

        assertEquals(List.of("db.example.com:27018"), seed(cfg));
    }

    @Test
    void multipleHostsAndTheDefaultPort() {
        MorphiumConfig cfg = new MorphiumConfig();
        Morphium.applyStandardConnectionUri(cfg, "mongodb://a.example:27017,b.example,c.example:27019");

        assertEquals(List.of("a.example:27017", "b.example:27017", "c.example:27019"), seed(cfg),
                "a host without a port defaults to 27017");
    }

    @Test
    void credentialsFromUserinfoArePercentDecoded() {
        MorphiumConfig cfg = new MorphiumConfig();
        // p%40ss decodes to p@ss - the @ inside the password must be encoded, so the userinfo
        // separator is unambiguous.
        Morphium.applyStandardConnectionUri(cfg, "mongodb://user:p%40ss@host.example:27017/db");

        assertEquals("user", cfg.authSettings().getMongoLogin());
        assertEquals("p@ss", cfg.authSettings().getMongoPassword());
        assertEquals(List.of("host.example:27017"), seed(cfg));
    }

    @Test
    void tlsAndReplicaSetAndAuthSourceFromTheQuery() {
        MorphiumConfig cfg = new MorphiumConfig();
        Morphium.applyStandardConnectionUri(cfg,
                "mongodb://host.example/db?tls=true&replicaSet=rs0&authSource=admin");

        assertTrue(cfg.connectionSettings().isUseSSL(), "tls=true must enable SSL");
        assertEquals("rs0", cfg.clusterSettings().getRequiredReplicaSetName());
        assertEquals("admin", cfg.authSettings().getMongoAuthDb());
    }

    @Test
    void sslAliasAlsoEnablesTls() {
        MorphiumConfig cfg = new MorphiumConfig();
        Morphium.applyStandardConnectionUri(cfg, "mongodb://host.example/?ssl=true");

        assertTrue(cfg.connectionSettings().isUseSSL());
    }

    @Test
    void explicitConfigTakesPrecedenceOverTheUri() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.authSettings().setMongoLogin("realUser");
        cfg.authSettings().setMongoAuthDb("realAuthDb");
        cfg.clusterSettings().setRequiredReplicaSetName("realRs");

        Morphium.applyStandardConnectionUri(cfg,
                "mongodb://uriUser:uriPass@host.example/uriDb?replicaSet=uriRs&authSource=uriAuth");

        assertEquals("realUser", cfg.authSettings().getMongoLogin(), "existing login must not be overwritten");
        assertEquals("realAuthDb", cfg.authSettings().getMongoAuthDb());
        assertEquals("realRs", cfg.clusterSettings().getRequiredReplicaSetName());
        // the host list, which was empty, is still taken from the URI
        assertEquals(List.of("host.example:27017"), seed(cfg));
    }

    @Test
    void aUriWithoutAnyHostIsRejected() {
        MorphiumConfig cfg = new MorphiumConfig();
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> Morphium.applyStandardConnectionUri(cfg, "mongodb://user:pass@/db"));
        assertTrue(ex.getMessage().contains("host"), ex.getMessage());
    }
}
