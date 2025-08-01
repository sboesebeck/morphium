package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class AuthSettings {
    private String mongoLogin = null, mongoPassword = null, mongoAuthDb = null;

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
}
