package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

public class AuthSettings extends Settings {
    private String mongoLogin = null, mongoPassword = null, mongoAuthDb = null;

    /**
     * Authentication mechanism.
     * {@code null} / {@code "SCRAM-SHA-256"} – standard username/password auth (default).
     * {@code "MONGODB-X509"} – X.509 client-certificate auth (no username/password needed;
     *  requires {@code useSSL=true} and a keystore containing the client certificate).
     */
    private String authMechanism = null;

    public String getMongoLogin() {
        return mongoLogin;
    }

    public AuthSettings setMongoLogin(String mongoLogin) {
        this.mongoLogin = mongoLogin;
        return this;
    }

    public String getMongoPassword() {
        return mongoPassword;
    }

    public AuthSettings setMongoPassword(String mongoPassword) {
        this.mongoPassword = mongoPassword;
        return this;
    }

    public String getMongoAuthDb() {
        return mongoAuthDb;
    }

    public AuthSettings setMongoAuthDb(String mongoAuthDb) {
        this.mongoAuthDb = mongoAuthDb;
        return this;
    }

    public String getAuthMechanism() {
        return authMechanism;
    }

    public AuthSettings setAuthMechanism(String authMechanism) {
        this.authMechanism = authMechanism;
        return this;
    }
}
