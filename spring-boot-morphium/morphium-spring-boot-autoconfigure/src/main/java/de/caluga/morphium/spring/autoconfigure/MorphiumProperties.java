package de.caluga.morphium.spring.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Binds every {@code morphium.*} key from {@code application.properties}/{@code .yml}
 * to a {@link de.caluga.morphium.MorphiumConfig} that {@link MorphiumAutoConfiguration}
 * uses to build the {@link de.caluga.morphium.Morphium} bean. Registered via
 * {@code @EnableConfigurationProperties(MorphiumProperties.class)} on
 * {@link MorphiumAutoConfiguration}, so it is only active together with that
 * auto-configuration (i.e. when {@code de.caluga.morphium.Morphium} is on the
 * classpath).
 *
 * <p>The property prefix is {@code morphium} (not {@code spring.morphium}) — the
 * {@code spring.*} namespace is reserved for Spring Boot's own configuration keys.
 * A minimal configuration only needs the database name and, unless the default
 * applies, the host list:
 *
 * <pre>{@code
 * morphium.database=my-database
 * morphium.hosts=localhost:27017
 * }</pre>
 *
 * <p>If {@code spring-boot-configuration-processor} is on the classpath (it is an
 * optional dependency of this module), every field below also appears in
 * {@code META-INF/spring-configuration-metadata.json}, giving IDEs autocompletion
 * and validation for {@code morphium.*} keys.
 */
@ConfigurationProperties(prefix = "morphium")
public class MorphiumProperties {

    /**
     * Comma-separated {@code host:port} list of MongoDB seed nodes, used unless
     * {@link #atlasUrl} is set (in which case {@link #atlasUrl} takes precedence).
     * Default: {@code localhost:27017}.
     */
    private List<String> hosts = List.of("localhost:27017");

    /**
     * Name of the MongoDB database Morphium connects to. Required — there is no
     * default; {@link MorphiumAutoConfiguration} passes this straight to
     * {@code MorphiumConfig.connectionSettings().setDatabase(...)}.
     */
    private String database;

    /**
     * MongoDB username. Only applied if both {@link #username} and {@link #password}
     * are non-null; no default (unset means no authentication).
     */
    private String username;

    /**
     * MongoDB password, applied together with {@link #username}. No default.
     */
    private String password;

    /**
     * Database against which {@link #username}/{@link #password} are authenticated
     * (MongoDB's {@code authSource}). Default: {@code admin}. Ignored unless
     * {@link #username} and {@link #password} are both set.
     */
    private String authDatabase = "admin";

    /**
     * Name of the Morphium driver implementation to use, e.g. {@code PooledDriver}
     * for production against a real MongoDB, or {@code InMemDriver} for tests that
     * run without a MongoDB instance. Default: {@code PooledDriver}.
     */
    private String driverName = "PooledDriver";

    /**
     * MongoDB read preference applied to the driver (e.g. {@code primary},
     * {@code secondary}, {@code primaryPreferred}). Default: {@code primary}.
     */
    private String readPreference = "primary";

    /**
     * Maximum number of pooled connections to MongoDB. Default: {@code 250}.
     */
    private int maxConnections = 250;

    /**
     * MongoDB Atlas SRV connection string. When set (non-null and non-blank), it
     * overrides {@link #hosts} entirely — {@link MorphiumAutoConfiguration} configures
     * the cluster from this URL instead of iterating {@link #hosts}. No default.
     */
    private String atlasUrl;

    /**
     * Name of the MongoDB replica set. Required for multi-document transactions
     * (see {@link MorphiumTransactional}); a standalone MongoDB node does not support
     * them. No default — if unset, Morphium connects without asserting a replica set
     * name.
     */
    private String replicaSetName;

    /**
     * Number of connection attempts {@link MorphiumAutoConfiguration} makes before
     * giving up when a transient connection error occurs (e.g. "no primary node
     * found", "not connected yet"). Retries use a linear backoff of
     * {@code attempt * 2000} milliseconds. Default: {@code 5}. Non-transient failures
     * are never retried and propagate immediately.
     */
    private int connectRetries = 5;

    /**
     * Index management strategy applied at startup, one of {@code CREATE_ON_STARTUP}
     * (create missing indexes eagerly), {@code WARN_ON_STARTUP} (log a warning instead
     * of creating), {@code CREATE_ON_WRITE_NEW_COL} (defer index creation to the first
     * write on a new collection), or {@code NO_CHECK} (skip index checking entirely).
     * Default: {@code CREATE_ON_STARTUP}.
     */
    private String indexCheck = "CREATE_ON_STARTUP";

    /**
     * Query result cache settings, bound under the {@code morphium.cache.*} prefix.
     */
    private CacheProperties cache = new CacheProperties();

    /**
     * TLS/SSL connection settings, bound under the {@code morphium.ssl.*} prefix.
     */
    private SslProperties ssl = new SslProperties();

    /**
     * Returns the configured MongoDB seed host list ({@code morphium.hosts}).
     *
     * @return comma-separated {@code host:port} entries; defaults to a single-element
     *         list containing {@code localhost:27017}
     */
    public List<String> getHosts() { return hosts; }

    /**
     * Sets the MongoDB seed host list bound from {@code morphium.hosts}. Ignored by
     * {@link MorphiumAutoConfiguration} if {@link #atlasUrl} is also set.
     *
     * @param hosts {@code host:port} entries to seed the MongoDB cluster connection
     */
    public void setHosts(List<String> hosts) { this.hosts = hosts; }

    /**
     * Returns the configured MongoDB database name ({@code morphium.database}).
     *
     * @return the database name, or {@code null} if not yet configured
     */
    public String getDatabase() { return database; }

    /**
     * Sets the MongoDB database name bound from {@code morphium.database}. This value
     * is required for {@link MorphiumAutoConfiguration} to build a working
     * {@code MorphiumConfig} — Morphium connects successfully with a {@code null}
     * database only in degenerate/test scenarios.
     *
     * @param database the database Morphium operates against
     */
    public void setDatabase(String database) { this.database = database; }

    /**
     * Returns the configured MongoDB username ({@code morphium.username}).
     *
     * @return the username, or {@code null} if authentication is not configured
     */
    public String getUsername() { return username; }

    /**
     * Sets the MongoDB username bound from {@code morphium.username}. Authentication
     * is only applied by {@link MorphiumAutoConfiguration} once both this and
     * {@link #password} are non-null.
     *
     * @param username the MongoDB username to authenticate with
     */
    public void setUsername(String username) { this.username = username; }

    /**
     * Returns the configured MongoDB password ({@code morphium.password}).
     *
     * @return the password, or {@code null} if authentication is not configured
     */
    public String getPassword() { return password; }

    /**
     * Sets the MongoDB password bound from {@code morphium.password}. See
     * {@link #setUsername(String)} for when it takes effect.
     *
     * @param password the MongoDB password to authenticate with
     */
    public void setPassword(String password) { this.password = password; }

    /**
     * Returns the authentication database ({@code morphium.auth-database}).
     *
     * @return the database MongoDB authenticates {@link #username}/{@link #password}
     *         against; defaults to {@code admin}
     */
    public String getAuthDatabase() { return authDatabase; }

    /**
     * Sets the authentication database bound from {@code morphium.auth-database}.
     *
     * @param authDatabase the MongoDB {@code authSource} database
     */
    public void setAuthDatabase(String authDatabase) { this.authDatabase = authDatabase; }

    /**
     * Returns the configured driver implementation name ({@code morphium.driver-name}).
     *
     * @return {@code PooledDriver}, {@code InMemDriver}, or another Morphium driver
     *         name; defaults to {@code PooledDriver}
     */
    public String getDriverName() { return driverName; }

    /**
     * Sets the driver implementation name bound from {@code morphium.driver-name}.
     * Use {@code InMemDriver} in tests to run against Morphium's in-memory MongoDB
     * emulation without a real MongoDB instance.
     *
     * @param driverName the Morphium driver implementation to instantiate
     */
    public void setDriverName(String driverName) { this.driverName = driverName; }

    /**
     * Returns the configured read preference ({@code morphium.read-preference}).
     *
     * @return the MongoDB read preference; defaults to {@code primary}
     */
    public String getReadPreference() { return readPreference; }

    /**
     * Sets the MongoDB read preference bound from {@code morphium.read-preference}.
     *
     * @param readPreference one of MongoDB's read preference names, e.g.
     *                       {@code primary}, {@code secondary}, {@code primaryPreferred}
     */
    public void setReadPreference(String readPreference) { this.readPreference = readPreference; }

    /**
     * Returns the configured connection pool size ({@code morphium.max-connections}).
     *
     * @return the maximum number of pooled MongoDB connections; defaults to {@code 250}
     */
    public int getMaxConnections() { return maxConnections; }

    /**
     * Sets the connection pool size bound from {@code morphium.max-connections}.
     *
     * @param maxConnections maximum number of pooled connections to MongoDB
     */
    public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }

    /**
     * Returns the configured MongoDB Atlas SRV URL ({@code morphium.atlas-url}).
     *
     * @return the Atlas connection string, or {@code null} if {@link #hosts} is used
     *         instead
     */
    public String getAtlasUrl() { return atlasUrl; }

    /**
     * Sets the MongoDB Atlas SRV URL bound from {@code morphium.atlas-url}. When set
     * to a non-blank value, {@link MorphiumAutoConfiguration} uses it instead of
     * {@link #hosts} to configure the cluster.
     *
     * @param atlasUrl the Atlas {@code mongodb+srv://...} connection string
     */
    public void setAtlasUrl(String atlasUrl) { this.atlasUrl = atlasUrl; }

    /**
     * Returns the configured replica set name ({@code morphium.replica-set-name}).
     *
     * @return the required replica set name, or {@code null} if not set
     */
    public String getReplicaSetName() { return replicaSetName; }

    /**
     * Sets the replica set name bound from {@code morphium.replica-set-name}. Required
     * for {@code @}{@link MorphiumTransactional} to work — MongoDB rejects
     * multi-document transactions on a standalone (non-replica-set) node.
     *
     * @param replicaSetName the MongoDB replica set name to require
     */
    public void setReplicaSetName(String replicaSetName) { this.replicaSetName = replicaSetName; }

    /**
     * Returns the configured connection retry count ({@code morphium.connect-retries}).
     *
     * @return the number of connection attempts before giving up; defaults to
     *         {@code 5}
     */
    public int getConnectRetries() { return connectRetries; }

    /**
     * Sets the connection retry count bound from {@code morphium.connect-retries}.
     * {@link MorphiumAutoConfiguration} only retries transient connection failures
     * (e.g. no primary elected yet); other exceptions propagate on the first attempt.
     *
     * @param connectRetries maximum number of connection attempts (at least 1 is
     *                       always attempted regardless of this value)
     */
    public void setConnectRetries(int connectRetries) { this.connectRetries = connectRetries; }

    /**
     * Returns the configured index check mode ({@code morphium.index-check}).
     *
     * @return one of {@code CREATE_ON_STARTUP}, {@code WARN_ON_STARTUP},
     *         {@code CREATE_ON_WRITE_NEW_COL}, {@code NO_CHECK}; defaults to
     *         {@code CREATE_ON_STARTUP}
     */
    public String getIndexCheck() { return indexCheck; }

    /**
     * Sets the index check mode bound from {@code morphium.index-check}. Any value
     * other than the four documented modes is silently ignored by
     * {@link MorphiumAutoConfiguration} (the underlying {@code MorphiumConfig} keeps
     * its own default in that case).
     *
     * @param indexCheck the index management strategy name
     */
    public void setIndexCheck(String indexCheck) { this.indexCheck = indexCheck; }

    /**
     * Returns the query result cache settings ({@code morphium.cache.*}).
     *
     * @return the nested cache configuration
     */
    public CacheProperties getCache() { return cache; }

    /**
     * Replaces the query result cache settings bound from {@code morphium.cache.*}.
     *
     * @param cache the nested cache configuration to use
     */
    public void setCache(CacheProperties cache) { this.cache = cache; }

    /**
     * Returns the TLS/SSL settings ({@code morphium.ssl.*}).
     *
     * @return the nested SSL configuration
     */
    public SslProperties getSsl() { return ssl; }

    /**
     * Replaces the TLS/SSL settings bound from {@code morphium.ssl.*}.
     *
     * @param ssl the nested SSL configuration to use
     */
    public void setSsl(SslProperties ssl) { this.ssl = ssl; }

    /**
     * Query result cache settings, bound under {@code morphium.cache.*} and applied
     * by {@link MorphiumAutoConfiguration} to
     * {@code MorphiumConfig.cacheSettings()}.
     */
    public static class CacheProperties {

        /**
         * Time-to-live, in milliseconds, for cached query results before Morphium
         * considers a cache entry invalid. Default: {@code 5000} (5 seconds).
         */
        private int globalValidTime = 5000;

        /**
         * Whether Morphium's read cache is enabled at all. When {@code false}, every
         * query bypasses the cache regardless of any per-query or per-entity cache
         * annotation. Default: {@code true}.
         */
        private boolean readCacheEnabled = true;

        /**
         * Returns the cache TTL ({@code morphium.cache.global-valid-time}).
         *
         * @return the cache validity duration in milliseconds; defaults to
         *         {@code 5000}
         */
        public int getGlobalValidTime() { return globalValidTime; }

        /**
         * Sets the cache TTL bound from {@code morphium.cache.global-valid-time}.
         *
         * @param globalValidTime cache validity duration in milliseconds
         */
        public void setGlobalValidTime(int globalValidTime) { this.globalValidTime = globalValidTime; }

        /**
         * Returns whether the read cache is enabled
         * ({@code morphium.cache.read-cache-enabled}).
         *
         * @return {@code true} if query results may be cached; defaults to
         *         {@code true}
         */
        public boolean isReadCacheEnabled() { return readCacheEnabled; }

        /**
         * Sets whether the read cache is enabled, bound from
         * {@code morphium.cache.read-cache-enabled}.
         *
         * @param readCacheEnabled {@code false} to disable query result caching
         *                        entirely
         */
        public void setReadCacheEnabled(boolean readCacheEnabled) { this.readCacheEnabled = readCacheEnabled; }
    }

    /**
     * TLS/SSL connection settings, bound under {@code morphium.ssl.*} and applied by
     * {@link MorphiumAutoConfiguration} when {@link #enabled} is {@code true}. Only
     * keystore-based client configuration is exposed here; truststore configuration
     * is not covered by this module.
     */
    public static class SslProperties {

        /**
         * Whether Morphium connects to MongoDB over TLS. Default: {@code false}. When
         * {@code true}, {@link MorphiumAutoConfiguration} builds an {@code SSLContext}
         * (using {@link #keystorePath}/{@link #keystorePassword} if set) and enables
         * it on the driver.
         */
        private boolean enabled = false;

        /**
         * Filesystem path to a keystore (JKS or PKCS12) holding the client
         * certificate/private key for TLS. No default. Only read when {@link #enabled}
         * is {@code true}; if {@code null} while {@link #enabled} is {@code true},
         * TLS is enabled without a client keystore.
         */
        private String keystorePath;

        /**
         * Password protecting {@link #keystorePath}. No default.
         */
        private String keystorePassword;

        /**
         * Returns whether TLS is enabled ({@code morphium.ssl.enabled}).
         *
         * @return {@code true} if Morphium connects over TLS; defaults to
         *         {@code false}
         */
        public boolean isEnabled() { return enabled; }

        /**
         * Sets whether TLS is enabled, bound from {@code morphium.ssl.enabled}.
         *
         * @param enabled {@code true} to connect to MongoDB over TLS
         */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        /**
         * Returns the configured keystore path ({@code morphium.ssl.keystore-path}).
         *
         * @return the keystore file path, or {@code null} if not configured
         */
        public String getKeystorePath() { return keystorePath; }

        /**
         * Sets the keystore path bound from {@code morphium.ssl.keystore-path}.
         *
         * @param keystorePath filesystem path to a JKS or PKCS12 keystore
         */
        public void setKeystorePath(String keystorePath) { this.keystorePath = keystorePath; }

        /**
         * Returns the configured keystore password
         * ({@code morphium.ssl.keystore-password}).
         *
         * @return the password protecting the keystore, or {@code null} if not
         *         configured
         */
        public String getKeystorePassword() { return keystorePassword; }

        /**
         * Sets the keystore password bound from {@code morphium.ssl.keystore-password}.
         *
         * @param keystorePassword password protecting {@link #keystorePath}
         */
        public void setKeystorePassword(String keystorePassword) { this.keystorePassword = keystorePassword; }
    }
}
