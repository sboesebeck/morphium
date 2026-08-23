/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.quarkus;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.ClassGraphCache;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.annotations.Capped;
import de.caluga.morphium.annotations.Driver;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Messaging;
import de.caluga.morphium.config.CollectionCheckSettings;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.wire.SslHelper;
import de.caluga.morphium.objectmapping.LocalDateTimeMapper;
import io.quarkus.runtime.ImageMode;
import io.quarkus.tls.TlsConfiguration;
import io.quarkus.tls.TlsConfigurationRegistry;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;
import de.caluga.morphium.quarkus.observability.MorphiumMetricsBinder;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producer for a single {@link Morphium} instance shared across the application.
 *
 * <p>Design principles:
 * <ul>
 *   <li>No {@code sun.*} or {@code jdk.internal.*} imports</li>
 *   <li>No {@link java.lang.reflect.Field#setAccessible} beyond what Morphium itself requires</li>
 *   <li>Lifecycle managed via CDI {@code @Observes} – no custom shutdown hooks</li>
 * </ul>
 */
@ApplicationScoped
public class MorphiumProducer {

    private static final Logger log = LoggerFactory.getLogger(MorphiumProducer.class);

    @Inject
    MorphiumRuntimeConfig config;

    @Inject
    Instance<TlsConfigurationRegistry> tlsRegistryInstance;

    // Kept as a field so the shutdown observer can close it.
    private volatile Morphium instance;

    @Produces
    @ApplicationScoped
    public Morphium morphium() {
        if (instance != null) {
            return instance;
        }
        synchronized (this) {
            if (instance != null) {
                return instance;
            }
            instance = buildMorphium();
        }
        return instance;
    }

    void onStop(@Observes ShutdownEvent event) {
        if (instance != null) {
            log.info("Closing Morphium connection on application shutdown");
            try {
                instance.close();
            } catch (Exception e) {
                log.warn("Error while closing Morphium", e);
            } finally {
                instance = null;
            }
        }

        // Deregister the metrics binder's gauges, if the Capability.METRICS-gated bean exists
        // at all (i.e. Micrometer is on the app's classpath). Uses the same Arc.container()
        // conditional lookup idiom as buildMorphium() -- see its Javadoc for why an unconditional
        // @Inject Instance<MorphiumMetricsBinder> field on this class is avoided.
        try (InstanceHandle<MorphiumMetricsBinder> binderHandle = Arc.container().instance(MorphiumMetricsBinder.class)) {
            if (binderHandle.isAvailable()) {
                try {
                    binderHandle.get().close();
                } catch (Exception e) {
                    log.warn("Error while deregistering Morphium metrics", e);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers – no reflection, no Unsafe
    // ------------------------------------------------------------------

    private void configureSsl(MorphiumConfig cfg, SslConfig ssl) {
        if (!ssl.enabled()) {
            return;
        }

        cfg.setUseSSL(true);
        cfg.setSslInvalidHostNameAllowed(ssl.invalidHostnameAllowed());

        // Auth mechanism (e.g. MONGODB-X509)
        ssl.authMechanism().ifPresent(cfg::setAuthMechanism);

        // Build SSLContext — explicit keystore/truststore paths take precedence,
        // then fall back to the Quarkus TLS registry (quarkus.tls.* properties).
        String keystorePath     = ssl.keystorePath().orElse(null);
        String keystorePassword = ssl.keystorePassword().orElse(null);
        String truststorePath     = ssl.truststorePath().orElse(null);
        String truststorePassword = ssl.truststorePassword().orElse(null);

        if (keystorePath != null || truststorePath != null) {
            // Explicit extension-specific paths — existing behavior
            try {
                SSLContext sslContext = SslHelper.createSslContext(
                        keystorePath, keystorePassword,
                        truststorePath, truststorePassword);
                cfg.setSslContext(sslContext);
                log.debug("SSLContext configured from keystore='{}', truststore='{}'",
                        keystorePath, truststorePath);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to build SSLContext from quarkus.morphium.ssl configuration: " + e.getMessage(), e);
            }
        } else {
            // No explicit paths — try Quarkus TLS registry (quarkus.tls.* properties)
            configureSslFromTlsRegistry(cfg, ssl);
        }

        // Explicit X.509 username (subject DN override)
        ssl.x509Username().ifPresent(dn -> {
            log.debug("Using explicit X.509 username (subject DN): {}", dn);
            cfg.authSettings().setMongoLogin(dn);
            // No password for X.509 – set empty to avoid SCRAM credential check
            cfg.authSettings().setMongoPassword("");
            cfg.authSettings().setMongoAuthDb("$external");
        });
    }

    /**
     * Attempts to configure the SSLContext when no explicit keystore/truststore paths
     * are set via {@code quarkus.morphium.ssl.*}.
     *
     * <p>Resolution strategy depends on the runtime mode:
     * <ul>
     *   <li><b>Explicit TLS name</b> ({@code quarkus.morphium.ssl.tls-configuration-name}):
     *       Always use the Quarkus TLS registry to look up the named configuration.</li>
     *   <li><b>Native mode</b> (no explicit name): Use the Quarkus TLS registry default.
     *       Native images cannot use {@code javax.net.ssl.*} JVM system properties;
     *       the native startup script ({@code run-quarkus-native.sh}) writes
     *       {@code quarkus.tls.key-store.p12.*} properties instead.</li>
     *   <li><b>JVM mode</b> (no explicit name): Use {@link SslHelper#createSslContext}
     *       with null paths, which reads the JVM default SSLContext honoring
     *       {@code javax.net.ssl.keyStore/trustStore} system properties set by the
     *       JVM startup script ({@code run-quarkus.sh} KEYSTORE_REGISTER mode).</li>
     * </ul>
     */
    private void configureSslFromTlsRegistry(MorphiumConfig cfg, SslConfig ssl) {
        // 1. Explicit TLS configuration name — always use the registry
        if (ssl.tlsConfigurationName().isPresent()) {
            configureSslFromNamedTlsConfig(cfg, ssl.tlsConfigurationName().get());
            return;
        }

        // 2. Native mode — use TLS registry default (javax.net.ssl.* not available)
        if (ImageMode.current() == ImageMode.NATIVE_RUN) {
            configureSslFromDefaultTlsRegistry(cfg);
            return;
        }

        // 3. JVM mode — do NOT set an explicit SSLContext.
        //    The Morphium driver will create its own default SSLContext which honors
        //    javax.net.ssl.keyStore/keyStorePassword/keyStoreType system properties
        //    set by the base-image startup script (run-quarkus.sh KEYSTORE_REGISTER).
        //    Setting SslHelper.createSslContext(null,null,null,null) would create an
        //    EMPTY SSLContext without client certificate, breaking X.509 auth.
        log.info("JVM mode: no explicit SSLContext set — driver will use javax.net.ssl.* system properties");
    }

    private void configureSslFromNamedTlsConfig(MorphiumConfig cfg, String name) {
        if (!tlsRegistryInstance.isResolvable()) {
            throw new IllegalStateException(
                    "Quarkus TLS registry not available but tls-configuration-name='" + name + "' is set");
        }
        TlsConfigurationRegistry tlsRegistry = tlsRegistryInstance.get();
        Optional<TlsConfiguration> tlsConfig;
        if ("<default>".equals(name)) {
            tlsConfig = tlsRegistry.getDefault();
        } else {
            tlsConfig = tlsRegistry.get(name);
        }
        if (tlsConfig.isEmpty()) {
            throw new IllegalStateException(
                    "Quarkus TLS configuration '" + name + "' not found. "
                    + "Ensure quarkus.tls."
                    + ("<default>".equals(name) ? "" : name + ".")
                    + "key-store.* / trust-store.* is configured.");
        }
        applySslContextFromTlsConfig(cfg, tlsConfig.get());
    }

    private void configureSslFromDefaultTlsRegistry(MorphiumConfig cfg) {
        if (!tlsRegistryInstance.isResolvable()) {
            log.debug("Quarkus TLS registry not available — no SSLContext configured");
            return;
        }
        TlsConfigurationRegistry tlsRegistry = tlsRegistryInstance.get();
        Optional<TlsConfiguration> tlsConfig = tlsRegistry.getDefault();
        if (tlsConfig.isPresent()) {
            applySslContextFromTlsConfig(cfg, tlsConfig.get());
        } else {
            log.debug("No default Quarkus TLS configuration found — SSLContext not configured");
        }
    }

    private void applySslContextFromTlsConfig(MorphiumConfig cfg, TlsConfiguration tlsConfig) {
        try {
            SSLContext sslContext = tlsConfig.createSSLContext();
            cfg.setSslContext(sslContext);
            String configName = tlsConfig.getName() != null ? tlsConfig.getName() : "<default>";
            log.info("SSLContext configured from Quarkus TLS registry (configuration: '{}')", configName);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create SSLContext from Quarkus TLS registry: " + e.getMessage(), e);
        }
    }

    /**
     * Parses the {@code quarkus.morphium.read-preference} string into a
     * {@link ReadPreference}. Accepted values match {@link MorphiumRuntimeConfig#readPreference()}'s
     * documentation: {@code primary}, {@code primaryPreferred}, {@code secondary},
     * {@code secondaryPreferred}, {@code nearest} (case-insensitive).
     *
     * @param value the configured read preference string
     * @return the corresponding {@link ReadPreference}; falls back to {@link ReadPreference#primary()}
     *         (matching the documented default) for an unrecognized value
     */
    static ReadPreference parseReadPreference(String value) {
        switch (value.toLowerCase()) {
            case "primary":
                return ReadPreference.primary();
            case "primarypreferred":
                return ReadPreference.primaryPreferred();
            case "secondary":
                return ReadPreference.secondary();
            case "secondarypreferred":
                return ReadPreference.secondaryPreferred();
            case "nearest":
                return ReadPreference.nearest();
            default:
                log.warn("Unrecognized quarkus.morphium.read-preference value '{}', falling back to 'primary'", value);
                return ReadPreference.primary();
        }
    }

    /**
     * Validates that {@code quarkus.morphium.username} and {@code quarkus.morphium.password}
     * are either both present or both absent.
     *
     * <p>Silently connecting unauthenticated when exactly one of the two is set would be a
     * serious, hard-to-notice misconfiguration: the application would appear to work (e.g.
     * against a no-auth MongoDB in dev) while every environment where auth is actually required
     * would either reject the connection outright, or — worse — silently succeed
     * unauthenticated against a MongoDB instance that happens to allow it.
     *
     * @param usernamePresent {@code config.username().isPresent()}
     * @param passwordPresent {@code config.password().isPresent()}
     * @throws IllegalStateException if exactly one of the two is present
     */
    static void validateCredentialsPresence(boolean usernamePresent, boolean passwordPresent) {
        if (usernamePresent != passwordPresent) {
            throw new IllegalStateException(
                    "quarkus.morphium." + (usernamePresent ? "password" : "username")
                            + " must also be set when quarkus.morphium."
                            + (usernamePresent ? "username" : "password")
                            + " is configured -- both or neither, never just one.");
        }
    }

    /**
     * Converts {@code quarkus.morphium.cache.global-valid-time} (a {@code long}, milliseconds)
     * to the {@code int} that {@code CacheSettings.setGlobalCacheValidTime(int)} actually takes.
     *
     * <p>A direct {@code (int)} cast silently overflows for any value above
     * {@code Integer.MAX_VALUE} ms (~24.8 days) — e.g. a well-intentioned "cache for 30 days"
     * config ({@code 2_592_000_000L} ms) would wrap to a negative int and produce a cache that
     * never (or immediately) expires, with no warning at all.
     *
     * @param globalValidTimeMs the configured value, in milliseconds
     * @return the same value, safely narrowed to {@code int}
     * @throws IllegalStateException if the value exceeds {@code Integer.MAX_VALUE}
     */
    static int toIntGlobalCacheValidTime(long globalValidTimeMs) {
        if (globalValidTimeMs > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "quarkus.morphium.cache.global-valid-time=" + globalValidTimeMs
                            + " exceeds the maximum supported value of " + Integer.MAX_VALUE
                            + " ms (~24.8 days) -- morphium-core's CacheSettings.globalCacheValidTime is an int.");
        }
        return (int) globalValidTimeMs;
    }

    /**
     * Applies the configured {@link MorphiumRuntimeConfig.IndexCheckMode} to {@code cfg},
     * accounting for the fact that {@code CREATE_ON_WRITE_NEW_COL} — unlike the other three
     * modes — sets both {@code IndexCheck} <em>and</em> {@code CappedCheck}
     * ({@code MorphiumConfig.setAutoIndexAndCappedCreationOnWrite(true)} sets both to
     * {@code CREATE_ON_WRITE_NEW_COL}, see {@code MorphiumConfig} lines ~508-511).
     *
     * <p>That matters because {@code Morphium.initializeAndConnect()} calls
     * {@code checkCapped()} <strong>unconditionally</strong> — with no mode gate at all, unlike
     * {@code checkIndices()}, which is only invoked for {@code CREATE_ON_STARTUP}/
     * {@code WARN_ON_STARTUP}. {@code checkCapped()} then calls
     * {@code ClassGraphCache.getClassesWithAnnotation(Capped.class.getName())}, which — despite
     * the build-time pre-registration this producer performs above — falls through to a live
     * ClassGraph classpath scan if that pre-registration is ever missing, cleared, or bypassed.
     * A live classpath scan is exactly what native-image cannot do at runtime (no classpath to
     * scan), so it crashes. Setting {@code CREATE_ON_WRITE_NEW_COL} alone (Stephan Boesebeck's
     * originally proposed fix) only prevents the {@code IndexCheck} scan and misses this
     * unconditional {@code CappedCheck} path entirely.
     *
     * <p><strong>Native image:</strong> both {@code IndexCheck} and {@code CappedCheck} are
     * forced to {@code NO_CHECK}. This deliberately gives up the on-write index/capped-creation
     * behaviour of this mode under native-image — accepted here because "starts reliably" beats
     * "creates indices automatically", and users who need that behaviour in native mode can
     * still call {@code ensureIndicesFor()}/create capped collections explicitly.
     *
     * <p><strong>JVM mode:</strong> deliberately left untouched (both checks stay
     * {@code CREATE_ON_WRITE_NEW_COL}). On the JVM the {@code checkCapped()} scan is not fatal —
     * it costs a one-time startup delay (same live-classpath scan the plain {@code morphium-core}
     * library always pays for this mode), and disabling {@code CappedCheck} here would silently
     * remove the mode's actual purpose (auto-creating capped collections on first write) for
     * every JVM-mode user, not just native-image ones. That regression would be worse than the
     * startup cost it avoids.
     *
     * @param cfg               the config being built
     * @param indexCheckMode    the (already native-image-downgraded, for {@code WARN_ON_STARTUP})
     *                          configured index check mode
     * @param imageMode         the current Quarkus {@link ImageMode}, used to decide whether the
     *                          native-image-only downgrade below applies
     */
    static void applyIndexCheckMode(MorphiumConfig cfg, MorphiumRuntimeConfig.IndexCheckMode indexCheckMode,
            ImageMode imageMode) {
        switch (indexCheckMode) {
            case CREATE_ON_STARTUP:
                // Disable Morphium-internal creation — Producer.ensureIndices() handles it
                cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.NO_CHECK);
                break;
            case WARN_ON_STARTUP:
                cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.WARN_ON_STARTUP);
                break;
            case CREATE_ON_WRITE_NEW_COL:
                cfg.setAutoIndexAndCappedCreationOnWrite(true);
                if (imageMode == ImageMode.NATIVE_RUN) {
                    // Defence in depth. setAutoIndexAndCappedCreationOnWrite(true) sets BOTH the
                    // index and the capped check to CREATE_ON_WRITE_NEW_COL (MorphiumConfig
                    // lines 509-511), and Morphium.initializeAndConnect() calls checkCapped()
                    // UNCONDITIONALLY -- unlike checkIndices(), which is gated to
                    // CREATE_ON_STARTUP/WARN_ON_STARTUP -- and checkCapped() would perform a live
                    // ClassGraph scan, which cannot work in a native image.
                    //
                    // In practice buildMorphium() already prevents that scan by pre-registering
                    // the build-time @Capped list into ClassGraphCache before the Morphium
                    // constructor runs (an empty list is enough: getClassesWithAnnotation returns
                    // the pre-registered entry and never reaches the scan). So this is not the
                    // sole safeguard -- but it is a cheap one that does not depend on that
                    // ordering staying intact, and it keeps this branch symmetric with the other
                    // three. Native runs cannot create collections on the fly anyway, so nothing
                    // of value is disabled here.
                    cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.NO_CHECK);
                    cfg.collectionCheckSettings().setCappedCheck(CollectionCheckSettings.CappedCheck.NO_CHECK);
                }
                break;
            case NO_CHECK:
                cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.NO_CHECK);
                break;
        }
    }

    private Morphium buildMorphium() {
        // Clear static caches and pre-register entities for the current ClassLoader.
        // This is essential for Quarkus dev-mode hot-reload where the QuarkusClassLoader
        // is replaced — without this, stale class references from the previous loader cause
        // ObjectMapperImpl/AnnotationAndReflectionHelper to silently skip all @Entity classes.
        // In production mode this is a harmless one-time init (clear of empty state + register).
        ObjectMapperImpl.clearEntityCache();
        AnnotationAndReflectionHelper.clearTypeIdCache();
        var entityNames = MorphiumRecorder.getMappedClassNames();
        if (!entityNames.isEmpty()) {
            AnnotationAndReflectionHelper.registerTypeIds(buildTypeIdMap(entityNames));
        }

        // Pre-populate ClassGraphCache with build-time discovered @Driver, @Messaging, and
        // @Capped classes. In GraalVM native mode there is no live classpath, so ClassGraph
        // finds nothing; the preRegister() call puts the entries into the cache map before
        // Morphium's constructor calls getClassesWithAnnotation(), causing the
        // computeIfAbsent to find the pre-populated list and skip the scan entirely.
        // Even an empty list is intentional for @Capped — it prevents checkCapped() from
        // falling through to a live ClassGraph scan.
        var driverNames = MorphiumRecorder.getDriverClassNames();
        if (driverNames.isEmpty()) {
            log.warn("Morphium: no @Driver classes were discovered at build time — "
                    + "the configured driver '{}' may not be found and Morphium may fall back "
                    + "to SingleMongoConnectDriver", config.driverName());
        }
        ClassGraphCache.preRegisterClassesWithAnnotation(Driver.class.getName(), driverNames);

        var messagingNames = MorphiumRecorder.getMessagingClassNames();
        ClassGraphCache.preRegisterClassesWithAnnotation(Messaging.class.getName(), messagingNames);

        var cappedNames = MorphiumRecorder.getCappedClassNames();
        ClassGraphCache.preRegisterClassesWithAnnotation(Capped.class.getName(), cappedNames);

        // Pre-register @Entity and @Embedded classes so ObjectMapperImpl can initialize
        // without triggering a live ClassGraph scan (which fails in native mode).
        // Unlike @Driver/@Messaging/@Capped (looked up by Morphium's constructor),
        // @Entity is looked up by ObjectMapperImpl.<init> and must also be pre-populated.
        var entityOnlyNames = MorphiumRecorder.getEntityClassNames();
        ClassGraphCache.preRegisterClassesWithAnnotation(Entity.class.getName(), entityOnlyNames);

        var embeddedOnlyNames = MorphiumRecorder.getEmbeddedClassNames();
        ClassGraphCache.preRegisterClassesWithAnnotation(Embedded.class.getName(), embeddedOnlyNames);

        MorphiumConfig cfg = new MorphiumConfig();

        cfg.connectionSettings().setDatabase(config.database());
        cfg.driverSettings().setDriverName(config.driverName());
        cfg.connectionSettings().setMaxConnections(config.maxConnections());
        cfg.connectionSettings().setMaxWaitTime(config.maxWaitTime());
        cfg.connectionSettings().setDefaultQueryTimeoutMS(config.defaultQueryTimeoutMs());
        cfg.driverSettings().setDefaultReadPreference(parseReadPreference(config.readPreference()));

        // Morphium's internal checkIndices() uses ClassGraph at startup.
        // In Quarkus, we handle index creation explicitly via ensureIndices() using the
        // build-time discovered entity list — so always disable Morphium's internal check
        // to avoid redundant index creation (Morphium + Producer would both call ensureIndicesFor).
        // WARN_ON_STARTUP calls checkIndices() → ClassGraphCache.getClassInfoWithAnnotation()
        // which bypasses the preRegister cache and triggers a live ClassGraph scan.
        // In native mode that scan crashes because there is no live classpath — so
        // WARN_ON_STARTUP must be downgraded to NO_CHECK in native images.
        MorphiumRuntimeConfig.IndexCheckMode effectiveIndexCheck = config.indexCheck();
        if (effectiveIndexCheck == MorphiumRuntimeConfig.IndexCheckMode.WARN_ON_STARTUP
                && ImageMode.current() == ImageMode.NATIVE_RUN) {
            log.warn("Morphium: indexCheck=WARN_ON_STARTUP is not supported in native images "
                    + "(checkIndices() calls ClassGraph directly, bypassing the preRegister cache). "
                    + "Downgrading to NO_CHECK for this native run.");
            effectiveIndexCheck = MorphiumRuntimeConfig.IndexCheckMode.NO_CHECK;
        }
        applyIndexCheckMode(cfg, effectiveIndexCheck, ImageMode.current());

        // Host configuration
        if (config.atlasUrl().isPresent()) {
            // Use ClusterSettings.setAtlasUrl() for mongodb+srv:// connection strings.
            // Morphium resolves the SRV record automatically in initializeAndConnect().
            cfg.clusterSettings().setAtlasUrl(config.atlasUrl().get());
        } else {
            for (String host : config.hosts()) {
                String trimmed = host.trim();
                if (!trimmed.isEmpty()) {
                    cfg.clusterSettings().addHostToSeed(trimmed);
                }
            }
        }

        // Replica set name (required for transactions)
        if (config.replicaSetName().isPresent()) {
            cfg.clusterSettings().setRequiredReplicaSetName(config.replicaSetName().get());
        }

        // Credentials
        validateCredentialsPresence(config.username().isPresent(), config.password().isPresent());
        if (config.username().isPresent() && config.password().isPresent()) {
            cfg.authSettings().setMongoLogin(config.username().get());
            cfg.authSettings().setMongoPassword(config.password().get());
            cfg.authSettings().setMongoAuthDb(config.authDatabase());
        }

        // Cache settings
        cfg.cacheSettings().setGlobalCacheValidTime(toIntGlobalCacheValidTime(config.cache().globalValidTime()));
        cfg.cacheSettings().setReadCacheEnabled(config.cache().readCacheEnabled());

        // TLS / X.509 settings
        configureSsl(cfg, config.ssl());

        log.info("Quarkus Morphium Extension v{} (Morphium {}, Jakarta Data {})",
            MorphiumVersion.extensionVersion(), MorphiumVersion.morphiumVersion(),
            MorphiumVersion.jakartaDataVersion());
        log.info("Creating Morphium connection to database '{}' (hosts: {}, driver: {}, replicaSetName: {}, ssl: {})",
            config.database(), config.hosts(), config.driverName(),
            config.replicaSetName().orElse("(none)"),
            config.ssl().enabled());

        Morphium m = connectWithRetry(cfg);

        // Register the blocking-call detector's storage listener only now, after the
        // connection has actually been established. Doing this here (instead of e.g. a
        // separate CDI StartupEvent observer that injects Instance<Morphium>) makes it
        // structurally impossible for the detector to itself be the cause of a connect —
        // by the time this line runs, `m` already exists.
        MorphiumBlockingCallDetector.registerOn(m);

        // Register Micrometer connection-pool/driver-stats gauges (Phase 1 MVP of the
        // observability plan, quarkus-morphium/docs/architecture/observability-module-plan.md
        // Section 4.1/6.1), but only if the Capability.METRICS-gated MorphiumMetricsBinder bean
        // was actually registered at build time (i.e. Micrometer is on the app's classpath) AND
        // the runtime kill-switch quarkus.morphium.observability.enabled (default true) is not
        // set to false -- see MorphiumObservabilityConfig for why this is a plain runtime
        // property, not a build-time one: it lets an app that has Micrometer on its classpath
        // for an unrelated reason opt out of Morphium's gauges specifically, without a rebuild.
        //
        // Local design decision: looked up via Arc.container().instance(...) rather than an
        // @Inject Instance<MorphiumMetricsBinder> field on this producer. An injected field
        // would still be safe to *declare* (CDI resolves Instance<T> lazily and Arc tolerates an
        // unsatisfied Instance<T> for an optional bean), but Arc.container().instance(...) is the
        // idiom already used by MorphiumRecorder (see its runMigrations()) for the same
        // "try-with-resources InstanceHandle from a plain non-observer method" API shape; that
        // precedent's beans are always-present, so isAvailable() here is new territory, added
        // specifically because MorphiumMetricsBinder may not be a bean at all. It keeps
        // MorphiumMetricsBinder resolution entirely inside this post-connect block rather than
        // adding another always-present field to this class -- this bean reference is only ever
        // needed here and in onStop().
        //
        // isAvailable() is the "bean may not exist" guard: when Capability.METRICS was absent at
        // build time, MorphiumMetricsBinder was never added as an AdditionalBeanBuildItem, so it
        // is simply not a bean at all — Arc.container().instance(...) returns a non-throwing
        // InstanceHandle whose isAvailable() is false, and no MeterRegistry/Micrometer type is
        // ever touched. On a dev-mode hot-reload, close() is called first to deregister the
        // previous Morphium instance's gauges before (conditionally) binding the new one (Section
        // 6.4 idempotency requirement) — bindTo() itself does not deduplicate across calls. close()
        // runs unconditionally whenever the bean exists, even if observability is currently
        // disabled, so a hot-reload that flips enabled=false->true->false leaves no stale gauges
        // from a previous, now-superseded Morphium instance either way.
        try (InstanceHandle<MorphiumMetricsBinder> binderHandle = Arc.container().instance(MorphiumMetricsBinder.class)) {
            if (binderHandle.isAvailable()) {
                MorphiumMetricsBinder binder = binderHandle.get();
                binder.close();
                if (config.observability().enabled()) {
                    binder.bindTo(m, config.database());
                }
            }
        }

        // Defensive: ensure the driver knows it's a replica set when a RS name is configured.
        // PooledDriver < 6.2.1 only checked host-seed count, missing single-node replica sets.
        if (config.replicaSetName().isPresent() && !m.getDriver().isReplicaSet()) {
            log.debug("Forcing replicaSet=true on driver (single-node replica set workaround)");
            m.getDriver().setReplicaSet(true);
        }

        log.info("Morphium connected (replicaSet: {}, replicaSetName: {})",
            m.getDriver().isReplicaSet(),
            m.getDriver().getReplicaSetName() != null ? m.getDriver().getReplicaSetName() : "(none)");

        // Override the default LocalDateTimeMapper with the configured format.
        // useBsonDate=true  → ISODate (native MongoDB dates, compatible with Morphia data)
        // useBsonDate=false → Map{sec, n} (legacy Morphium format)
        m.getMapper().registerCustomMapperFor(LocalDateTime.class,
                new LocalDateTimeMapper(config.localDateTime().useBsonDate()));

        // Morphium's built-in index creation uses ClassGraph which does not work
        // with Quarkus's classloader. Use the entity classes discovered at build time
        // and explicitly ensure their indexes — but only when configured to do so.
        if (config.indexCheck() == MorphiumRuntimeConfig.IndexCheckMode.CREATE_ON_STARTUP) {
            ensureIndices(m);
        }

        return m;
    }

    /**
     * Creates a Morphium instance with retry logic. In containerized CI environments
     * (e.g. Docker-in-Docker), the MongoDB replica set primary may not be immediately
     * reachable after the container reports ready. This method retries the connection
     * with linear backoff (2s, 4s, 6s, ...) to handle transient startup delays.
     */
    private Morphium connectWithRetry(MorphiumConfig cfg) {
        int maxAttempts = Math.max(1, config.connectRetries());
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return new Morphium(cfg);
            } catch (Exception e) {
                boolean isTransient = isTransientConnectionError(e);
                if (!isTransient || attempt == maxAttempts) {
                    throw e;
                }
                long delayMs = attempt * 2000L;
                log.warn("Morphium connection attempt {}/{} failed: {}. Retrying in {}ms...",
                        attempt, maxAttempts, e.getMessage(), delayMs);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying Morphium connection", ie);
                }
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private static boolean isTransientConnectionError(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("No primary node found")
                    || msg.contains("not connected yet"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * Builds a typeId→FQCN map from the entity class names discovered at build time.
     * Loads each class, reads its @Entity/@Embedded annotation, and extracts the typeId.
     */
    private Map<String, String> buildTypeIdMap(List<String> classNames) {
        Map<String, String> typeIds = new HashMap<>();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (String cn : classNames) {
            try {
                Class<?> cls = Class.forName(cn, false, cl);
                Entity entity = cls.getAnnotation(Entity.class);
                if (entity != null) {
                    if (!".".equals(entity.typeId())) {
                        typeIds.put(entity.typeId(), cn);
                    }
                    typeIds.put(cn, cn);
                }
                Embedded embedded = cls.getAnnotation(Embedded.class);
                if (embedded != null) {
                    if (!".".equals(embedded.typeId())) {
                        typeIds.put(embedded.typeId(), cn);
                    }
                    typeIds.put(cn, cn);
                }
            } catch (ClassNotFoundException e) {
                log.warn("Could not load entity class for type ID registration: {}", cn);
            }
        }
        return typeIds;
    }

    /**
     * Ensures MongoDB indexes for all {@code @Entity} classes discovered at build time.
     *
     * <p><b>Important:</b> This must iterate only {@code @Entity} classes, not the combined
     * {@code @Entity}+{@code @Embedded} list from {@link MorphiumRecorder#getMappedClassNames()}.
     * {@code Morphium.ensureIndicesFor()} calls {@code ObjectMapperImpl.getCollectionName()},
     * which throws {@code IllegalArgumentException} for {@code @Embedded}-only classes
     * (they have no collection name). Using {@link MorphiumRecorder#getEntityClassNames()} avoids this.
     */
    private void ensureIndices(Morphium m) {
        for (String className : MorphiumRecorder.getEntityClassNames()) {
            try {
                Class<?> entityClass = Thread.currentThread().getContextClassLoader().loadClass(className);
                m.ensureIndicesFor(entityClass);
                log.debug("Ensured indexes for {}", className);
            } catch (ClassNotFoundException e) {
                log.warn("Could not load entity class for index creation: {}", className);
            } catch (Exception e) {
                log.warn("Failed to ensure indexes for entity class {}: {}", className, e.getMessage(), e);
            }
        }
    }
}
