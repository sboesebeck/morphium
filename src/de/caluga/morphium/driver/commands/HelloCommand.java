package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class HelloCommand extends MongoCommand<HelloCommand> {
    private String saslSupportedMechs;
    private Boolean helloOk = true;
    private boolean includeClient = true;
    private Boolean loadBalanced;

    public HelloCommand(MongoConnection d) {
        super(d);
        setDb("");
        setColl("");
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
        ret.put("$db", "local");
        String driverName = "unknown";
        if (getConnection() != null && getConnection()!=null && getConnection().getDriver()!=null) {
            driverName = getConnection().getDriver().getName();
        }
        if (includeClient) {
            ret.put("client", Doc.of("application", Doc.of("name", "Morphium"),
                    "driver", Doc.of("name", driverName, "version", "1.0"),
                    "os", Doc.of("type", System.getProperty("os.name"))));
        }
        return ret;
    }


    public HelloResult execute() throws MorphiumDriverException {
        var msg = getConnection().sendCommand(this);
        var crs = getConnection().getAnswerFor(msg, getDefaultBatchSize());
        return HelloResult.fromMsg(crs.next());
    }


}
