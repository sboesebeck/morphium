package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration that creates the application's single {@link Morphium} bean from
 * {@link MorphiumProperties} ({@code morphium.*} keys). It applies only when
 * {@code de.caluga.morphium.Morphium} is on the classpath
 * ({@code @ConditionalOnClass(Morphium.class)}); on a plain Spring Boot application
 * with the {@code morphium-spring-boot-starter} dependency, this is always the case.
 *
 * <p>Adding the starter and configuring at least {@code morphium.database} is enough
 * to get a connected, injectable {@code Morphium} instance:
 *
 * <pre>{@code
 * // application.properties
 * morphium.database=my-database
 * morphium.hosts=localhost:27017
 * }</pre>
 *
 * <pre>{@code
 * @Service
 * public class ProductService {
 *     @Autowired Morphium morphium;
 * }
 * }</pre>
 *
 * <h2>Overriding the {@code Morphium} bean</h2>
 * The {@link #morphium(MorphiumProperties)} bean method is annotated
 * {@code @ConditionalOnMissingBean}: if the application context already defines its
 * own {@code Morphium} bean (of any name), this auto-configuration backs off entirely
 * and its bean method is never invoked. This is the standard Spring Boot
 * "auto-configuration as a default, not a mandate" pattern — define your own
 * {@code @Bean Morphium morphium(...)} to take full control of connection setup while
 * still using every other part of this module ({@link EnableMorphiumRepositories},
 * {@link MorphiumTransactional}, the actuator health indicator).
 *
 * <p>Connection retries: transient failures during the initial connection attempt
 * (MongoDB not yet electing a primary, or not yet accepting connections) are retried
 * up to {@link MorphiumProperties#getConnectRetries()} times with a linear backoff of
 * {@code attempt * 2000} milliseconds; non-transient failures propagate on the first
 * attempt.
 */
@AutoConfiguration
@ConditionalOnClass(Morphium.class)
@EnableConfigurationProperties(MorphiumProperties.class)
public class MorphiumAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MorphiumAutoConfiguration.class);

    /**
     * Builds and connects the application's {@link Morphium} instance from
     * {@code properties}: it builds a {@code MorphiumConfig} from {@code properties}
     * (see {@link #buildConfig}) and connects with retry (see {@link #connectWithRetry}).
     *
     * <p>Only runs if no other {@code Morphium} bean is already defined in the context
     * ({@code @ConditionalOnMissingBean}) — see the class-level documentation for how
     * to supply your own.
     *
     * @param properties the bound {@code morphium.*} configuration
     * @return a connected {@code Morphium} instance, ready for injection
     * @throws RuntimeException (or a Morphium-specific subtype) if the connection
     *         cannot be established within {@link MorphiumProperties#getConnectRetries()}
     *         attempts, or if building an SSL context from
     *         {@link MorphiumProperties.SslProperties} fails
     */
    @Bean
    @ConditionalOnMissingBean
    public Morphium morphium(MorphiumProperties properties) {
        MorphiumConfig cfg = buildConfig(properties);
        Morphium m = connectWithRetry(cfg, properties.getConnectRetries());

        if (properties.getReplicaSetName() != null && !m.getDriver().isReplicaSet()) {
            log.debug("Forcing replicaSet=true on driver (single-node replica set workaround)");
            m.getDriver().setReplicaSet(true);
        }

        log.info("Morphium connected to '{}' (driver: {}, replicaSet: {})",
                properties.getDatabase(), properties.getDriverName(),
                m.getDriver().isReplicaSet());

        return m;
    }

    /**
     * Translates every {@link MorphiumProperties} field into the corresponding
     * {@code MorphiumConfig} setting: database, driver name, connection pool size,
     * read preference, index-check mode, host list (or Atlas URL if configured, which
     * then takes precedence over the host list), replica set name, credentials, cache
     * settings, and — if {@code morphium.ssl.enabled} is {@code true} — an SSL context
     * built from the configured keystore.
     *
     * @param properties the bound {@code morphium.*} configuration
     * @return a fully populated {@code MorphiumConfig}, not yet connected
     * @throws IllegalStateException if {@code morphium.ssl.enabled} is {@code true}
     *         and building the {@code SSLContext} from the configured keystore fails
     */
    private MorphiumConfig buildConfig(MorphiumProperties properties) {
        MorphiumConfig cfg = new MorphiumConfig();

        cfg.connectionSettings().setDatabase(properties.getDatabase());
        cfg.driverSettings().setDriverName(properties.getDriverName());
        cfg.connectionSettings().setMaxConnections(properties.getMaxConnections());
        cfg.driverSettings().setDefaultReadPreferenceType(properties.getReadPreference());

        // Index check mode
        switch (properties.getIndexCheck()) {
            case "CREATE_ON_STARTUP":
                cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.CREATE_ON_STARTUP);
                break;
            case "WARN_ON_STARTUP":
                cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.WARN_ON_STARTUP);
                break;
            case "CREATE_ON_WRITE_NEW_COL":
                cfg.setAutoIndexAndCappedCreationOnWrite(true);
                break;
            case "NO_CHECK":
                cfg.collectionCheckSettings().setIndexCheck(CollectionCheckSettings.IndexCheck.NO_CHECK);
                break;
        }

        // Host configuration
        if (properties.getAtlasUrl() != null && !properties.getAtlasUrl().isBlank()) {
            cfg.clusterSettings().setAtlasUrl(properties.getAtlasUrl());
        } else {
            for (String host : properties.getHosts()) {
                String trimmed = host.trim();
                if (!trimmed.isEmpty()) {
                    cfg.clusterSettings().addHostToSeed(trimmed);
                }
            }
        }

        // Replica set name
        if (properties.getReplicaSetName() != null && !properties.getReplicaSetName().isBlank()) {
            cfg.clusterSettings().setRequiredReplicaSetName(properties.getReplicaSetName());
        }

        // Credentials
        if (properties.getUsername() != null && properties.getPassword() != null) {
            cfg.authSettings().setMongoLogin(properties.getUsername());
            cfg.authSettings().setMongoPassword(properties.getPassword());
            cfg.authSettings().setMongoAuthDb(properties.getAuthDatabase());
        }

        // Cache
        cfg.cacheSettings().setGlobalCacheValidTime(properties.getCache().getGlobalValidTime());
        cfg.cacheSettings().setReadCacheEnabled(properties.getCache().isReadCacheEnabled());

        // SSL
        if (properties.getSsl().isEnabled()) {
            cfg.setUseSSL(true);
            String keystorePath = properties.getSsl().getKeystorePath();
            String keystorePassword = properties.getSsl().getKeystorePassword();
            if (keystorePath != null) {
                try {
                    var sslContext = de.caluga.morphium.driver.wire.SslHelper.createSslContext(
                            keystorePath, keystorePassword, null, null);
                    cfg.setSslContext(sslContext);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to build SSLContext: " + e.getMessage(), e);
                }
            }
        }

        return cfg;
    }

    /**
     * Attempts to construct a connected {@link Morphium} instance from {@code cfg},
     * retrying up to {@code maxRetries} times (at least once, regardless of the value
     * passed) when {@link #isTransient(Throwable)} recognizes the failure as
     * transient. Each retry waits {@code attempt * 2000} milliseconds before trying
     * again.
     *
     * @param cfg the configuration to connect with
     * @param maxRetries maximum number of connection attempts; values less than 1 are
     *                    treated as 1
     * @return a connected {@code Morphium} instance
     * @throws RuntimeException the original exception from the last attempt, if every
     *         attempt failed with a transient error, or immediately if an attempt
     *         failed with a non-transient error
     * @throws IllegalStateException never thrown in practice — present only to satisfy
     *         the compiler after the retry loop, which always returns or throws
     */
    private Morphium connectWithRetry(MorphiumConfig cfg, int maxRetries) {
        int maxAttempts = Math.max(1, maxRetries);
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return new Morphium(cfg);
            } catch (Exception e) {
                if (!isTransient(e) || attempt == maxAttempts) {
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

    /**
     * Walks the exception's cause chain looking for messages that indicate a
     * transient MongoDB connection state ("No primary node found", "not connected
     * yet") rather than a permanent configuration or authentication error.
     *
     * @param t the throwable raised while connecting
     * @return {@code true} if any exception in the cause chain matches a known
     *         transient-failure message
     */
    private static boolean isTransient(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("No primary node found") || msg.contains("not connected yet"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
