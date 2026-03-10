package de.caluga.morphium.driver.commands.auth;

import de.caluga.morphium.driver.commands.AdminMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateUserAdminCommand extends AdminMongoCommand<CreateUserAdminCommand> {
    private String userName;
    private String pwd;
    private Map<String, Object> customData;
    private List<Map<String, String>> roles = new ArrayList<>();
    private List<AuthenticationRestrictions> authenticationRestrictions;
    private boolean digestPassword; //if true, send unencrypted, the server "digests"
    private List<String> mechanisms;


    public CreateUserAdminCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "createUser";
    }

    public String getUserName() {
        return userName;
    }

    public CreateUserAdminCommand setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public String getPwd() {
        return pwd;
    }

    public CreateUserAdminCommand setPwd(String pwd) {
        this.pwd = pwd;
        return this;
    }

    public Map<String, Object> getCustomData() {
        return customData;
    }

    public CreateUserAdminCommand setCustomData(Map<String, Object> customData) {
        this.customData = customData;
        return this;
    }

    public List<Map<String, String>> getRoles() {
        return roles;
    }

    public CreateUserAdminCommand setRoles(List<Map<String, String>> roles) {
        this.roles = roles;
        return this;
    }

    public List<AuthenticationRestrictions> getAuthenticationRestrictions() {
        return authenticationRestrictions;
    }

    public CreateUserAdminCommand setAuthenticationRestrictions(List<AuthenticationRestrictions> authenticationRestrictions) {
        this.authenticationRestrictions = authenticationRestrictions;
        return this;
    }

    public boolean isDigestPassword() {
        return digestPassword;
    }

    public CreateUserAdminCommand setDigestPassword(boolean digestPassword) {
        this.digestPassword = digestPassword;
        return this;
    }

    public List<String> getMechanisms() {
        return mechanisms;
    }

    public CreateUserAdminCommand setMechanisms(List<String> mechanisms) {
        this.mechanisms = mechanisms;
        return this;
    }

    private static class AuthenticationRestrictions {
        private List<String> source;
        private List<String> serverAddress;

        public List<String> getSource() {
            return source;
        }

        public AuthenticationRestrictions setSource(List<String> source) {
            this.source = source;
            return this;
        }

        public List<String> getServerAddress() {
            return serverAddress;
        }

        public AuthenticationRestrictions setServerAddress(List<String> serverAddress) {
            this.serverAddress = serverAddress;
            return this;
        }
    }
}
