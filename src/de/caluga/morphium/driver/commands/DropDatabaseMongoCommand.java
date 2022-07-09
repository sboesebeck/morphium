package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DropDatabaseMongoCommand extends MongoCommand<DropDatabaseMongoCommand> implements SingleResultCommand {
    private Map<String, Object> writeConcern;

    public DropDatabaseMongoCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public DropDatabaseMongoCommand setColl(String coll) {
        LoggerFactory.getLogger(DropDatabaseMongoCommand.class).warn("Cannot set collection on DB command");
        return this;
    }

    @Override
    public String getCommandName() {
        return "dropDatabase";
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put(getCommandName(), 1);
        return m;
    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        //noinspection unchecked
        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
            setMetaData(Doc.of("server", connection.getConnectedTo()));
            long start = System.currentTimeMillis();
            var msg = connection.sendCommand(asMap());
            var crs = connection.readSingleAnswer(msg);
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return crs;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
}
