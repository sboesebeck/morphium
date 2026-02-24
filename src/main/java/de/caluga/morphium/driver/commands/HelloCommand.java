package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HelloCommand extends MongoCommand<HelloCommand> {
    private String saslSupportedMechs;
    private Boolean helloOk = true;
    private boolean includeClient = true;
    private Boolean loadBalanced;
    @Transient
    private String authDb;
    @Transient
    private String user;

    public HelloCommand(MongoConnection d) {
        super(d);
        setDb("");
        setColl("");
    }

    public String getAuthDb() {
        return authDb;
    }

    public HelloCommand setAuthDb(String authDb) {
        this.authDb = authDb;
        return this;
    }

    public String getUser() {
        return user;
    }

    public HelloCommand setUser(String user) {
        this.user = user;
        return this;
    }

    public Boolean getLoadBalanced() {
        return loadBalanced;
    }

    public HelloCommand setLoadBalanced(Boolean loadBalanced) {
        this.loadBalanced = loadBalanced;
        return this;
    }

    public Boolean getHelloOk() {
        return helloOk;
    }

    public HelloCommand setHelloOk(Boolean helloOk) {
        this.helloOk = helloOk;
        return this;
    }

    public boolean isIncludeClient() {
        return includeClient;
    }

    public HelloCommand setIncludeClient(boolean includeClient) {
        this.includeClient = includeClient;
        return this;
    }

    public String getSaslSupportedMechs() {
        return saslSupportedMechs;
    }

    public HelloCommand setSaslSupportedMechs(String dbDotUser) {
        this.saslSupportedMechs = dbDotUser;
        return this;
    }

    @Override
    public String getCommandName() {
        return "hello";
    }

    @Override
    public HelloCommand setDb(String db) {
        //not setting db!
        return this;
    }

    @Override
    public Map<String, Object> asMap() {
        var ret = super.asMap();
        ret.put(getCommandName(), 1);
        if (authDb != null) {
            ret.put("saslSupportedMechs", authDb + "." + user);
        }
        ret.put("$db", "admin");
        String driverName = "unknown";
        if (getConnection() != null && getConnection().getDriver() != null) {
            driverName = getConnection().getDriver().getName();
        }
        if (includeClient) {
            ret.put("client", Doc.of("application", Doc.of("name", "Morphium"),
                                     "driver", Doc.of("name", "Morphium V6/" + driverName, "version", "6.1"),
                                     "os", Doc.of("type", System.getProperty("os.name"))));
        }
        // Advertise supported compressors to the server (MongoDB wire protocol compression negotiation)
        List<String> compressors = new ArrayList<>();
        compressors.add("snappy");
        compressors.add("zlib");
        ret.put("compression", compressors);
        return ret;
    }


    public HelloResult execute() throws MorphiumDriverException {
        var msg = getConnection().sendCommand(this);
        var crs = getConnection().readSingleAnswer(msg);
        return HelloResult.fromMsg(crs);
    }


}
