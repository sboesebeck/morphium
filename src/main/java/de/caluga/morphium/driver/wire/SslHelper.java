package de.caluga.morphium.driver.wire;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Helper class for creating SSLContext instances from keystores.
 * <p>
 * Usage example:
 * <pre>
 * SSLContext sslContext = SslHelper.createSslContext(
 *     "/path/to/keystore.jks", "keystorePassword",
 *     "/path/to/truststore.jks", "truststorePassword"
 * );
 * driver.setSslContext(sslContext);
 * driver.setUseSSL(true);
 * </pre>
 */
public class SslHelper {

    /**
     * Creates an SSLContext from keystore and truststore files.
     *
     * @param keystorePath     Path to the keystore file (JKS or PKCS12), or null to skip
     * @param keystorePassword Password for the keystore
     * @param truststorePath   Path to the truststore file, or null to use system default
     * @param truststorePassword Password for the truststore
     * @return Configured SSLContext
     */
    public static SSLContext createSslContext(String keystorePath, String keystorePassword,
                                               String truststorePath, String truststorePassword)
            throws IOException, GeneralSecurityException {

        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = null;

        // Load keystore for client/server certificate
        if (keystorePath != null) {
            KeyStore keyStore = loadKeyStore(keystorePath, keystorePassword);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keystorePassword != null ? keystorePassword.toCharArray() : null);
            keyManagers = kmf.getKeyManagers();
        }

        // Load truststore for certificate validation
        if (truststorePath != null) {
            KeyStore trustStore = loadKeyStore(truststorePath, truststorePassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            trustManagers = tmf.getTrustManagers();
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, new SecureRandom());
        return sslContext;
    }

    /**
     * Creates an SSLContext with only a truststore (for client connections).
     *
     * @param truststorePath     Path to the truststore file
     * @param truststorePassword Password for the truststore
     * @return Configured SSLContext
     */
    public static SSLContext createClientSslContext(String truststorePath, String truststorePassword)
            throws IOException, GeneralSecurityException {
        return createSslContext(null, null, truststorePath, truststorePassword);
    }

    /**
     * Creates an SSLContext with only a keystore (for server connections).
     *
     * @param keystorePath     Path to the keystore file
     * @param keystorePassword Password for the keystore
     * @return Configured SSLContext
     */
    public static SSLContext createServerSslContext(String keystorePath, String keystorePassword)
            throws IOException, GeneralSecurityException {
        return createSslContext(keystorePath, keystorePassword, null, null);
    }

    /**
     * Creates an SSLContext that trusts all certificates (INSECURE - for testing only).
     *
     * @return SSLContext that accepts any certificate
     */
    public static SSLContext createTrustAllSslContext() throws GeneralSecurityException {
        TrustManager[] trustAllCerts = new TrustManager[]{
            new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return new java.security.cert.X509Certificate[0];
                }
                public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                }
                public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                }
            }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new SecureRandom());
        return sslContext;
    }

    private static KeyStore loadKeyStore(String path, String password)
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
        // Determine keystore type from extension
        String type = path.toLowerCase().endsWith(".p12") || path.toLowerCase().endsWith(".pfx")
                ? "PKCS12" : "JKS";

        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream is = new FileInputStream(path)) {
            keyStore.load(is, password != null ? password.toCharArray() : null);
        }
        return keyStore;
    }
}
