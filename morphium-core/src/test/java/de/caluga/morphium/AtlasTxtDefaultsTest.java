package de.caluga.morphium;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies how MongoDB DNS-seedlist TXT options are applied to a {@link MorphiumConfig}
 * (issue #169). Lives in the production package to exercise the package-private
 * {@link Morphium#applyAtlasTxtDefaults}.
 */
@Tag("core")
public class AtlasTxtDefaultsTest {

    private static Map<String, String> opts(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void appliesAuthSourceAndReplicaSetWhenUnset() {
        MorphiumConfig cfg = new MorphiumConfig();

        Morphium.applyAtlasTxtDefaults(cfg, opts("authsource", "admin", "replicaset", "atlas-rs0"));

        assertEquals("admin", cfg.authSettings().getMongoAuthDb());
        assertEquals("atlas-rs0", cfg.clusterSettings().getRequiredReplicaSetName());
    }

    @Test
    void userConfiguredValuesTakePrecedence() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.authSettings().setMongoAuthDb("myAuthDb");
        cfg.clusterSettings().setRequiredReplicaSetName("myRs");

        Morphium.applyAtlasTxtDefaults(cfg, opts("authsource", "admin", "replicaset", "atlas-rs0"));

        assertEquals("myAuthDb", cfg.authSettings().getMongoAuthDb(), "existing authSource must not be overwritten");
        assertEquals("myRs", cfg.clusterSettings().getRequiredReplicaSetName(), "existing replicaSet must not be overwritten");
    }

    @Test
    void unsupportedOptionsAreIgnoredWithoutError() {
        MorphiumConfig cfg = new MorphiumConfig();

        assertDoesNotThrow(() -> Morphium.applyAtlasTxtDefaults(cfg, opts("loadbalanced", "true", "authsource", "admin")));

        assertEquals("admin", cfg.authSettings().getMongoAuthDb());
    }

    @Test
    void emptyOptionsAreNoOp() {
        MorphiumConfig cfg = new MorphiumConfig();
        String authBefore = cfg.authSettings().getMongoAuthDb();
        String rsBefore   = cfg.clusterSettings().getRequiredReplicaSetName();

        Morphium.applyAtlasTxtDefaults(cfg, opts());

        assertEquals(authBefore, cfg.authSettings().getMongoAuthDb());
        assertEquals(rsBefore, cfg.clusterSettings().getRequiredReplicaSetName());
    }
}
