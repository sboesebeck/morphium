package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.NetworkCallHelper;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DropDatabaseMongoCommand extends MongoCommand<DropDatabaseMongoCommand> implements SingleResultCommand {
    private Map<String, Object> writeConcern;

    public DropDatabaseMongoCommand(MorphiumDriver d) {
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
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
            setMetaData(Doc.of("server", driver.getHostSeed().get(0)));
            long start = System.currentTimeMillis();
            MorphiumCursor crs = driver.runCommand(getDb(), asMap()).getCursor();
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return crs.next();
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }
}
