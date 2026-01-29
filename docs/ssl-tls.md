# SSL/TLS Connections

Morphium v6 supports encrypted connections to MongoDB via SSL/TLS. This is essential for production deployments and required by MongoDB Atlas.

## Quick Start

### Enable SSL (System Trust Store)

For MongoDB instances with certificates signed by a public CA (like Atlas):

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.clusterSettings()
   .addHostToSeed("cluster0.example.mongodb.net", 27017);
cfg.connectionSettings()
   .setUseSSL(true);

Morphium morphium = new Morphium(cfg);
```

### With Custom Trust Store

For self-signed certificates or private CAs:

```java
import de.caluga.morphium.driver.wire.SslHelper;

SSLContext sslContext = SslHelper.createClientSslContext(
    "/path/to/truststore.jks",   // Trust store path
    "truststorePassword"          // Trust store password
);

MorphiumConfig cfg = new MorphiumConfig();
cfg.clusterSettings()
   .addHostToSeed("mongo.internal", 27017);
cfg.connectionSettings()
   .setUseSSL(true)
   .setSslContext(sslContext);
```

### With Client Certificate (mTLS)

For mutual TLS authentication:

```java
SSLContext sslContext = SslHelper.createSslContext(
    "/path/to/keystore.jks",      // Client certificate
    "keystorePassword",
    "/path/to/truststore.jks",    // Server CA certificates
    "truststorePassword"
);

cfg.connectionSettings()
   .setUseSSL(true)
   .setSslContext(sslContext);
```

## Configuration Options

| Setting | Default | Description |
|---------|---------|-------------|
| `setUseSSL(boolean)` | `false` | Enable SSL/TLS encryption |
| `setSslContext(SSLContext)` | `null` | Custom SSLContext (optional) |
| `setSslInvalidHostNameAllowed(boolean)` | `false` | Allow hostname mismatches (testing only!) |

## SslHelper Methods

The `SslHelper` utility class provides convenient methods for creating SSLContext instances:

```java
import de.caluga.morphium.driver.wire.SslHelper;

// Client connection with custom CA
SSLContext ctx = SslHelper.createClientSslContext(truststorePath, password);

// Server mode with certificate
SSLContext ctx = SslHelper.createServerSslContext(keystorePath, password);

// Full mTLS with both keystore and truststore
SSLContext ctx = SslHelper.createSslContext(
    keystorePath, keystorePassword,
    truststorePath, truststorePassword
);

// Trust all certificates (TESTING ONLY - INSECURE!)
SSLContext ctx = SslHelper.createTrustAllSslContext();
```

## Keystore Formats

SslHelper automatically detects the keystore format:

- **JKS** (`.jks`) — Java KeyStore (default)
- **PKCS12** (`.p12`, `.pfx`) — Industry standard format

### Converting PEM to JKS

If you have PEM files from MongoDB/Atlas:

```bash
# Convert CA certificate to truststore
keytool -import -trustcacerts -alias mongodb-ca \
    -file ca.pem \
    -keystore truststore.jks \
    -storepass changeit

# Convert client cert + key to PKCS12, then to JKS
openssl pkcs12 -export \
    -in client.pem -inkey client-key.pem \
    -out client.p12 -name mongodb-client \
    -password pass:changeit

keytool -importkeystore \
    -srckeystore client.p12 -srcstoretype PKCS12 \
    -destkeystore keystore.jks -deststoretype JKS \
    -srcstorepass changeit -deststorepass changeit
```

## MongoDB Atlas Example

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.clusterSettings()
   .addHostToSeed("cluster0-shard-00-00.abc123.mongodb.net", 27017)
   .addHostToSeed("cluster0-shard-00-01.abc123.mongodb.net", 27017)
   .addHostToSeed("cluster0-shard-00-02.abc123.mongodb.net", 27017)
   .setRequiredReplicaSetName("atlas-abc123-shard-0");
   
cfg.connectionSettings()
   .setDatabase("mydb")
   .setUseSSL(true);  // Atlas requires SSL
   
cfg.authSettings()
   .setMongoLogin("atlasUser")
   .setMongoPassword("atlasPassword")
   .setMongoAuthDb("admin");

Morphium morphium = new Morphium(cfg);
```

Or use a connection URI:

```java
// URI parsing handles SSL automatically for mongodb+srv://
MorphiumConfig cfg = new MorphiumConfig();
// ... parse from URI or set manually
```

## Troubleshooting

### Certificate Errors

**"PKIX path building failed"** — The server certificate is not trusted.

- Add the CA certificate to your truststore
- Or use `SslHelper.createClientSslContext()` with a custom truststore

**"Hostname verification failed"** — Certificate CN doesn't match hostname.

```java
// For testing only! Don't use in production!
cfg.connectionSettings().setSslInvalidHostNameAllowed(true);
```

### Testing with Self-Signed Certs

For development/testing environments with self-signed certificates:

```java
// ⚠️ INSECURE - Never use in production!
SSLContext trustAll = SslHelper.createTrustAllSslContext();
cfg.connectionSettings()
   .setUseSSL(true)
   .setSslContext(trustAll)
   .setSslInvalidHostNameAllowed(true);
```

## v5 vs v6 Comparison

| Feature | v5.x | v6.x |
|---------|------|------|
| SSL/TLS support | ❌ Not available | ✅ Full support |
| Custom SSLContext | ❌ | ✅ |
| mTLS (client certs) | ❌ | ✅ |
| SslHelper utility | ❌ | ✅ |
| MongoDB Atlas | ❌ | ✅ |
