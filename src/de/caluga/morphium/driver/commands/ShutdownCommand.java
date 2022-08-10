package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;

import java.util.Map;

public class ShutdownCommand extends AdminMongoCommand<ShutdownCommand> {
    private Boolean force;
    private Integer timeoutSecs;

    public ShutdownCommand(MongoConnection d) {
        super(d);

    }

    public Boolean getForce() {
        return force;
    }

    public ShutdownCommand setForce(Boolean force) {
        this.force = force;
        return this;
    }

    public Integer getTimeoutSecs() {
        return timeoutSecs;
    }

    public ShutdownCommand setTimeoutSecs(Integer timeoutSecs) {
        this.timeoutSecs = timeoutSecs;
        return this;
    }

    @Override
    public String getCommandName() {
        return "shutdown";
    }

    @Override
    public Map<String, Object> asMap() {
        var m= super.asMap();
        m.put(getCommandName(),1);
        return m;
    }


    public Map<String, Object> execute() throws MorphiumDriverException {
       int msg=executeAsync();
       return getConnection().readSingleAnswer(msg);

    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        //noinspection unchecked
        return new NetworkCallHelper<Integer>().doCall(() -> {
            MongoConnection connection = getConnection();
            //noinspection unchecked

            setMetaData("server", connection.getConnectedTo());
            long start = System.currentTimeMillis();
            var msg = connection.sendCommand(this);
            return msg;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
}
