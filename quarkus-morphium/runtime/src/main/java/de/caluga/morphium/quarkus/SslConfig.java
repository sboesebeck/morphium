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

import io.smallrye.config.WithDefault;

import java.util.Optional;

/**
 * TLS / X.509 configuration group, nested under {@link MorphiumRuntimeConfig#ssl()}.
 *
 * <h3>TLS-only (encrypted transport, server certificate validation):</h3>
 * <pre>{@code
 * quarkus.morphium.ssl.enabled=true
 * quarkus.morphium.ssl.truststore-path=/etc/certs/mongo-truststore.jks
 * quarkus.morphium.ssl.truststore-password=changeit
 * }</pre>
 *
 * <h3>X.509 client-certificate authentication (MongoDB Atlas):</h3>
 * <pre>{@code
 * quarkus.morphium.ssl.enabled=true
 * quarkus.morphium.ssl.auth-mechanism=MONGODB-X509
 * quarkus.morphium.ssl.keystore-path=/etc/certs/client-keystore.p12
 * quarkus.morphium.ssl.keystore-password=secret
 * quarkus.morphium.ssl.truststore-path=/etc/certs/mongo-truststore.jks
 * quarkus.morphium.ssl.truststore-password=changeit
 * # Optional – overrides the subject DN extracted from the certificate:
 * # morphium.ssl.x509-username=CN=myUser,O=myOrg,C=DE
 * }</pre>
 */
public interface SslConfig {

    /** Whether TLS is enabled for the MongoDB connection. Default: {@code false}. */
    @WithDefault("false")
    boolean enabled();

    /**
     * Authentication mechanism.
     * <ul>
     *   <li>Absent / {@code SCRAM-SHA-256} – standard username/password auth (default).</li>
     *   <li>{@code MONGODB-X509} – X.509 client-certificate authentication.
     *       Requires {@link #enabled() ssl.enabled=true} and a keystore ({@link #keystorePath()})
     *       containing the client certificate.</li>
     * </ul>
     */
    Optional<String> authMechanism();

    /**
     * Path to the keystore file (JKS or PKCS12) containing the client certificate
     * for X.509 authentication.  Also used for mutual TLS.
     */
    Optional<String> keystorePath();

    /** Password for the keystore. */
    Optional<String> keystorePassword();

    /**
     * Path to the truststore file used to validate the MongoDB server certificate.
     * When absent the JVM default truststore is used.
     */
    Optional<String> truststorePath();

    /** Password for the truststore. */
    Optional<String> truststorePassword();

    /**
     * Allow invalid / self-signed hostnames in the server certificate.
     * <strong>Do not set in production.</strong> Default: {@code false}.
     */
    @WithDefault("false")
    boolean invalidHostnameAllowed();

    /**
     * Explicit X.509 subject DN to use as the MongoDB username.
     * When absent the subject DN is extracted automatically from the client certificate
     * presented during the TLS handshake.
     * Example: {@code CN=myUser,OU=myUnit,O=myOrg,C=DE}
     */
    Optional<String> x509Username();

    /**
     * Name of a Quarkus TLS configuration (from {@code quarkus.tls.<name>.*}) to use
     * for the MongoDB connection. The SSLContext is obtained from the Quarkus TLS registry
     * instead of from explicit keystore/truststore paths.
     *
     * <p>Use the special value {@code <default>} to explicitly select the unnamed default
     * TLS configuration.
     *
     * <p>When absent and no explicit {@link #keystorePath()} / {@link #truststorePath()}
     * is configured, the extension automatically falls back to the default (unnamed) Quarkus
     * TLS configuration if one is available. This is the recommended setup for native images
     * where the runtime script writes {@code quarkus.tls.key-store.p12.*} /
     * {@code quarkus.tls.trust-store.p12.*} properties.
     */
    Optional<String> tlsConfigurationName();
}
